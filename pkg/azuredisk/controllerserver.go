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
	"context"
	"errors"
	"fmt"
	"strings"

	"k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/cloudprovider/providers/azure"
	"k8s.io/kubernetes/pkg/volume/util"

	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2018-10-01/compute"
	"github.com/Azure/azure-sdk-for-go/services/storage/mgmt/2018-07-01/storage"
	"github.com/andyzhangx/azurefile-csi-driver/pkg/csi-common"
	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/golang/glog"
	"github.com/pborman/uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	volumeIDTemplate = "%s#%s#%s"
)

var (
	// volumeCaps represents how the volume could be accessed.
	// It is SINGLE_NODE_WRITER since azure disk could only be attached to a single node at any given time.
	volumeCaps = []csi.VolumeCapability_AccessMode{
		{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
	}

	// controllerCaps represents the capability of controller service
	controllerCaps = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
	}
)

type controllerServer struct {
	*csicommon.DefaultControllerServer
	cloud *azure.Cloud
}

func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		glog.Errorf("invalid create volume req: %v", req)
		return nil, err
	}

	// Validate arguments
	volumeCapabilities := req.GetVolumeCapabilities()
	name := req.GetName()
	if len(name) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume Name must be provided")
	}
	if volumeCapabilities == nil || len(volumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume Volume capabilities must be provided")
	}

	if !cs.isValidVolumeCapabilities(volumeCapabilities) {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities not supported")
	}

	volSizeBytes := int64(req.GetCapacityRange().GetRequiredBytes())
	requestGiB := int(util.RoundUpSize(volSizeBytes, 1024*1024*1024))

	var (
		location, account  string
		storageAccountType string
		cachingMode        v1.AzureDataDiskCachingMode
		strKind            string
		err                error
		resourceGroup      string
		diskIopsReadWrite  string
		diskMbpsReadWrite  string
	)

	parameters := req.GetParameters()
	for k, v := range parameters {
		switch strings.ToLower(k) {
		case "skuname":
			storageAccountType = v
		case "location":
			location = v
		case "storageaccount":
			account = v
		case "storageaccounttype":
			storageAccountType = v
		case "kind":
			strKind = v
		case "cachingmode":
			cachingMode = v1.AzureDataDiskCachingMode(v)
		case "resourcegroup":
			resourceGroup = v
			/* new zone implementation in csi, these parameters are not needed
			case "zone":
				zonePresent = true
				availabilityZone = v
			case "zones":
				zonesPresent = true
				availabilityZones, err = util.ZonesToSet(v)
				if err != nil {
					return nil, fmt.Errorf("error parsing zones %s, must be strings separated by commas: %v", v, err)
				}
			case "zoned":
				strZoned = v
			*/
		case "diskiopsreadwrite":
			diskIopsReadWrite = v
		case "diskmbpsreadwrite":
			diskMbpsReadWrite = v
		default:
			return nil, fmt.Errorf("AzureDisk - invalid option %s in storage class", k)
		}
	}

	// maxLength = 79 - (4 for ".vhd") = 75
	// todo: get cluster name
	diskName := util.GenerateVolumeName("pvc-disk", uuid.NewUUID().String(), 75)

	// normalize values
	skuName, err := normalizeStorageAccountType(storageAccountType)
	if err != nil {
		return nil, err
	}

	kind, err := normalizeKind(strFirstLetterToUpper(strKind))
	if err != nil {
		return nil, err
	}

	if kind != v1.AzureManagedDisk {
		if resourceGroup != "" {
			return nil, errors.New("StorageClass option 'resourceGroup' can be used only for managed disks")
		}
	}

	selectedAvailabilityZone := pickAvailabilityZone(req.GetAccessibilityRequirements())

	if cachingMode, err = normalizeCachingMode(cachingMode); err != nil {
		return nil, err
	}

	// create disk
	glog.V(2).Infof("begin to create azure disk(%s) account type(%s) rg(%s) location(%s) size(%d)", diskName, skuName, resourceGroup, location, requestGiB)

	diskURI := ""
	if kind == v1.AzureManagedDisk {
		tags := make(map[string]string)
		/* todo: check where are the tags in CSI
		if p.options.CloudTags != nil {
			tags = *(p.options.CloudTags)
		}
		*/

		volumeOptions := &azure.ManagedDiskOptions{
			DiskName:           diskName,
			StorageAccountType: skuName,
			ResourceGroup:      resourceGroup,
			PVCName:            "",
			SizeGB:             requestGiB,
			Tags:               tags,
			AvailabilityZone:   selectedAvailabilityZone,
			DiskIOPSReadWrite:  diskIopsReadWrite,
			DiskMBpsReadWrite:  diskMbpsReadWrite,
		}
		diskURI, err = cs.cloud.CreateManagedDisk(volumeOptions)
		if err != nil {
			return nil, err
		}
	} else {
		if kind == v1.AzureDedicatedBlobDisk {
			_, diskURI, _, err = cs.cloud.CreateVolume(name, account, storageAccountType, location, requestGiB)
			if err != nil {
				return nil, err
			}
		} else {
			diskURI, err = cs.cloud.CreateBlobDisk(name, storage.SkuName(storageAccountType), requestGiB)
			if err != nil {
				return nil, err
			}
		}
	}

	glog.V(2).Infof("create azure disk(%s) account type(%s) rg(%s) location(%s) size(%d) successfully", diskName, skuName, resourceGroup, location, requestGiB)

	/*  todo: block volume support
	if utilfeature.DefaultFeatureGate.Enabled(features.BlockVolume) {
		volumeMode = p.options.PVC.Spec.VolumeMode
		if volumeMode != nil && *volumeMode == v1.PersistentVolumeBlock {
			// Block volumes should not have any FSType
			fsType = ""
		}
	}
	*/

	if req.GetVolumeContentSource() != nil {
		contentSource := req.GetVolumeContentSource()
		if contentSource.GetSnapshot() != nil {
		}
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			Id:            diskURI,
			CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
			Attributes:    parameters,
			AccessibleTopology: []*csi.Topology{
				{
					Segments: map[string]string{topologyKey: selectedAvailabilityZone},
				},
			},
		},
	}, nil
}

func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		return nil, fmt.Errorf("invalid delete volume req: %v", req)
	}
	volumeID := req.VolumeId

	resourceGroupName, accountName, fileShareName, err := getFileShareInfo(volumeID)
	if err != nil {
		return nil, err
	}

	if resourceGroupName == "" {
		resourceGroupName = cs.cloud.ResourceGroup
	}

	accountKey, err := GetStorageAccesskey(cs.cloud, accountName, resourceGroupName)
	if err != nil {
		return nil, fmt.Errorf("no key for storage account(%s) under resource group(%s), err %v", accountName, resourceGroupName, err)
	}

	glog.V(2).Infof("deleting azure file(%s) rg(%s) account(%s) volumeID(%s)", fileShareName, resourceGroupName, accountName, volumeID)
	if err := cs.cloud.DeleteFileShare(accountName, accountKey, fileShareName); err != nil {
		return nil, err
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if req.GetVolumeCapabilities() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities missing in request")
	}
	if _, ok := azureFileVolumes[req.GetVolumeId()]; !ok {
		return nil, status.Error(codes.NotFound, "Volume does not exist")
	}

	for _, cap := range req.VolumeCapabilities {
		if cap.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
			return &csi.ValidateVolumeCapabilitiesResponse{Supported: false, Message: ""}, nil
		}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{Supported: true, Message: ""}, nil
}

func (cs *controllerServer) isValidVolumeCapabilities(volCaps []*csi.VolumeCapability) bool {
	hasSupport := func(cap *csi.VolumeCapability) bool {
		for _, c := range volumeCaps {
			// todo: Block volume support
			/* compile error here
			if blk := c.GetBlock(); blk != nil {
				return false
			}
			*/
			if c.GetMode() == cap.AccessMode.GetMode() {
				return true
			}
		}
		return false
	}

	foundAll := true
	for _, c := range volCaps {
		if !hasSupport(c) {
			foundAll = false
		}
	}
	return foundAll
}

func normalizeKind(kind string) (v1.AzureDataDiskKind, error) {
	if kind == "" {
		return defaultAzureDiskKind, nil
	}

	if !supportedDiskKinds.Has(kind) {
		return "", fmt.Errorf("azureDisk - %s is not supported disk kind. Supported values are %s", kind, supportedDiskKinds.List())
	}

	return v1.AzureDataDiskKind(kind), nil
}

func normalizeStorageAccountType(storageAccountType string) (compute.DiskStorageAccountTypes, error) {
	if storageAccountType == "" {
		return defaultStorageAccountType, nil
	}

	sku := compute.DiskStorageAccountTypes(storageAccountType)
	supportedSkuNames := compute.PossibleDiskStorageAccountTypesValues()
	for _, s := range supportedSkuNames {
		if sku == s {
			return sku, nil
		}
	}

	return "", fmt.Errorf("azureDisk - %s is not supported sku/storageaccounttype. Supported values are %s", storageAccountType, supportedSkuNames)
}

func normalizeCachingMode(cachingMode v1.AzureDataDiskCachingMode) (v1.AzureDataDiskCachingMode, error) {
	if cachingMode == "" {
		return defaultAzureDataDiskCachingMode, nil
	}

	if !supportedCachingModes.Has(string(cachingMode)) {
		return "", fmt.Errorf("azureDisk - %s is not supported cachingmode. Supported values are %s", cachingMode, supportedCachingModes.List())
	}

	return cachingMode, nil
}

func strFirstLetterToUpper(str string) string {
	if len(str) < 2 {
		return str
	}
	return strings.ToUpper(string(str[0])) + str[1:]
}

// pickAvailabilityZone selects 1 zone given topology requirement.
// if not found, empty string is returned.
func pickAvailabilityZone(requirement *csi.TopologyRequirement) string {
	if requirement == nil {
		return ""
	}
	for _, topology := range requirement.GetPreferred() {
		zone, exists := topology.GetSegments()[topologyKey]
		if exists {
			return zone
		}
	}
	for _, topology := range requirement.GetRequisite() {
		zone, exists := topology.GetSegments()[topologyKey]
		if exists {
			return zone
		}
	}
	return ""
}
