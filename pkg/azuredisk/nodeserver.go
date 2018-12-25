/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package azuredisk

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/golang/glog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"golang.org/x/net/context"

	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume/util"
)

const (
	defaultFsType = "ext4"
)

func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	targetPath := req.GetTargetPath()
	notMnt, err := mount.New("").IsLikelyNotMountPoint(targetPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	mounter := mount.New("")
	if !notMnt {
		// testing original mount point, make sure the mount link is valid
		if _, err := ioutil.ReadDir(targetPath); err == nil {
			glog.V(2).Infof("azureFile - already mounted to target %s", targetPath)
			return &csi.NodePublishVolumeResponse{}, nil
		}
		// todo: mount link is invalid, now unmount and remount later (built-in functionality)
		glog.Warningf("azureFile - ReadDir %s failed with %v, unmount this directory", targetPath, err)
		if err := mounter.Unmount(targetPath); err != nil {
			glog.Errorf("azureFile - Unmount directory %s failed with %v", targetPath, err)
			return nil, err
		}
		notMnt = true
	}

	fsType := req.GetVolumeCapability().GetMount().GetFsType()

	readOnly := req.GetReadonly()
	volumeID := req.GetVolumeId()
	attrib := req.GetVolumeAttributes()
	mountFlags := req.GetVolumeCapability().GetMount().GetMountFlags()

	glog.V(2).Infof("target %v\nfstype %v\n\nreadonly %v\nvolumeId %v\nattributes %v\nmountflags %v\n",
		targetPath, fsType, readOnly, volumeID, attrib, mountFlags)

	var accountName, accountKey, fileShareName string

	secrets := req.GetNodePublishSecrets()
	if len(secrets) == 0 {
		var resourceGroupName string
		resourceGroupName, accountName, fileShareName, err = getFileShareInfo(volumeID)
		if err != nil {
			return nil, err
		}

		if resourceGroupName == "" {
			resourceGroupName = d.cloud.ResourceGroup
		}

		accountKey, err = GetStorageAccesskey(d.cloud, accountName, resourceGroupName)
		if err != nil {
			return nil, fmt.Errorf("no key for storage account(%s) under resource group(%s), err %v", accountName, resourceGroupName, err)
		}
	} else {
		for k, v := range attrib {
			switch strings.ToLower(k) {
			case "sharename":
				fileShareName = v
			}
		}
		if fileShareName == "" {
			return nil, fmt.Errorf("could not find sharename from attributes(%v)", attrib)
		}

		accountName, accountKey, err = getStorageAccount(secrets)
		if err != nil {
			return nil, err
		}
	}

	mountOptions := []string{}
	source := ""
	osSeparator := string(os.PathSeparator)
	source = fmt.Sprintf("%s%s%s.file.%s%s%s", osSeparator, osSeparator, accountName, d.cloud.Environment.StorageEndpointSuffix, osSeparator, fileShareName)

	if runtime.GOOS == "windows" {
		mountOptions = []string{fmt.Sprintf("AZURE\\%s", accountName), accountKey}
	} else {
		if err := os.MkdirAll(targetPath, 0700); err != nil {
			return nil, err
		}
		// parameters suggested by https://azure.microsoft.com/en-us/documentation/articles/storage-how-to-use-files-linux/
		options := []string{fmt.Sprintf("username=%s,password=%s", accountName, accountKey)}
		if readOnly {
			options = append(options, "ro")
		}
		mountOptions = util.JoinMountOptions(mountFlags, options)
		mountOptions = appendDefaultMountOptions(mountOptions)
	}

	err = mounter.Mount(source, targetPath, "cifs", mountOptions)
	if err != nil {
		notMnt, mntErr := mounter.IsLikelyNotMountPoint(targetPath)
		if mntErr != nil {
			glog.Errorf("IsLikelyNotMountPoint check failed: %v", mntErr)
			return nil, err
		}
		if !notMnt {
			if mntErr = mounter.Unmount(targetPath); mntErr != nil {
				glog.Errorf("Failed to unmount: %v", mntErr)
				return nil, err
			}
			notMnt, mntErr := mounter.IsLikelyNotMountPoint(targetPath)
			if mntErr != nil {
				glog.Errorf("IsLikelyNotMountPoint check failed: %v", mntErr)
				return nil, err
			}
			if !notMnt {
				// This is very odd, we don't expect it.  We'll try again next sync loop.
				glog.Errorf("%s is still mounted, despite call to unmount().  Will try again next sync loop.", targetPath)
				return nil, err
			}
		}
		os.Remove(targetPath)
		return nil, err
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}
	targetPath := req.GetTargetPath()
	volumeID := req.GetVolumeId()

	// Unmounting the image
	err := mount.New("").Unmount(req.GetTargetPath())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	glog.V(4).Infof("azuredisk: volume %s/%s has been unmounted.", targetPath, volumeID)

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (d *Driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	glog.V(4).Infof("NodeStageVolume: called with args %+v", *req)
	diskURI := req.GetVolumeId()
	if len(diskURI) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	target := req.GetStagingTargetPath()
	if len(target) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Staging target not provided")
	}

	volCap := req.GetVolumeCapability()
	if volCap == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability not provided")
	}

	if !isValidVolumeCapabilities([]*csi.VolumeCapability{volCap}) {
		return nil, status.Error(codes.InvalidArgument, "Volume capability not supported")
	}

	source, ok := req.PublishInfo["devicePath"]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "Device path not provided")
	}

	// todo: move this struct to the common driver struct
	mounter := &mount.SafeFormatAndMount{
		Interface: mount.New(""),
		Exec:      mount.NewOsExec(),
	}
	// TODO: consider replacing IsLikelyNotMountPoint by IsNotMountPoint
	notMnt, err := mounter.IsLikelyNotMountPoint(target)
	if err != nil {
		if os.IsNotExist(err) {
			if errMkDir := mounter.MakeDir(target); errMkDir != nil {
				msg := fmt.Sprintf("could not create target dir %q: %v", target, errMkDir)
				return nil, status.Error(codes.Internal, msg)
			}
			notMnt = true
		} else {
			msg := fmt.Sprintf("could not determine if %q is valid mount point: %v", target, err)
			return nil, status.Error(codes.Internal, msg)
		}
	}

	if !notMnt {
		msg := fmt.Sprintf("target %q is not a valid mount point", target)
		return nil, status.Error(codes.InvalidArgument, msg)
	}
	// Get fs type that the volume will be formatted with
	attributes := req.GetVolumeAttributes()
	fsType, exists := attributes["fsType"]
	if !exists || fsType == "" {
		fsType = defaultFsType
	}

	// FormatAndMount will format only if needed
	glog.V(2).Infof("NodeStageVolume: formatting %s and mounting at %s", source, target)

	err = mounter.FormatAndMount(source, target, fsType, nil)
	if err != nil {
		msg := fmt.Sprintf("could not format %q and mount it at %q", source, target)
		return nil, status.Error(codes.Internal, msg)
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (d *Driver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetStagingTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}


func (d *Driver) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	glog.V(2).Infof("Using default NodeGetCapabilities")

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: d.NSCap,
	}, nil
}

func (d *Driver) NodeGetId(ctx context.Context, req *csi.NodeGetIdRequest) (*csi.NodeGetIdResponse, error) {
	glog.V(5).Infof("Using default NodeGetId")

	return &csi.NodeGetIdResponse{
		NodeId: d.NodeID,
	}, nil
}

func (d *Driver) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	glog.V(5).Infof("Using default NodeGetInfo")

	return &csi.NodeGetInfoResponse{
		NodeId: d.NodeID,
	}, nil
}