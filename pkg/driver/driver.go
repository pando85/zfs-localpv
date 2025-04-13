/*
Copyright © 2019 The OpenEBS Authors

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

package driver

import (
	"github.com/container-storage-interface/spec/lib/go/csi"
	config "github.com/openebs/zfs-localpv/pkg/config"
	"k8s.io/klog/v2"
)

// CSIDriver defines a common data structure
// for drivers
// TODO check if this can be renamed to Base
type CSIDriver struct {
	// TODO change the field names to make it
	// readable
	config *config.Config
	ids    csi.IdentityServer
	ns     csi.NodeServer
	cs     csi.ControllerServer

	cap []*csi.VolumeCapability_AccessMode
}

// GetVolumeCapabilityAccessModes fetches the access
// modes on which the volume can be exposed
func GetVolumeCapabilityAccessModes() []*csi.VolumeCapability_AccessMode {
	supported := []csi.VolumeCapability_AccessMode_Mode{
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
	}

	var vcams []*csi.VolumeCapability_AccessMode
	for _, vcam := range supported {
		klog.Infof("enabling volume access mode: %s", vcam.String())
		vcams = append(vcams, newVolumeCapabilityAccessMode(vcam))
	}
	return vcams
}

func newVolumeCapabilityAccessMode(mode csi.VolumeCapability_AccessMode_Mode) *csi.VolumeCapability_AccessMode {
	return &csi.VolumeCapability_AccessMode{Mode: mode}
}

// New returns a new driver instance
func New(config *config.Config) *CSIDriver {
	driver := &CSIDriver{
		config: config,
		cap:    GetVolumeCapabilityAccessModes(),
	}

	switch config.PluginType {
	case "controller":
		driver.cs = NewController(driver)

	case "agent":
		// Start monitor goroutine to monitor the
		// ZfsVolume CR. If there is any event
		// related to the volume like destroy or
		// property change, handle it accordingly.

		driver.ns = NewNode(driver)
	case "backupGC":
		// Initialize and run the backup garbage collector
		// to monitor and clean up orphaned backups
		klog.Info("Initializing backup garbage collector")
		bgc := &BackupGarbageCollector{}
		if err := bgc.Initialize(); err != nil {
			klog.Fatalf("Failed to initialize backup garbage collector: %v", err)
		}
		klog.Info("Backup garbage collector initialized successfully")
	}

	// Identity server is common to both node and
	// controller, it is required to register,
	// share capabilities and probe the corresponding
	// driver
	driver.ids = NewIdentity(driver)
	return driver
}

// Run starts the CSI plugin by communicating
// over the given endpoint
func (d *CSIDriver) Run() error {
	// For backupGC plugin type, we don't need to start a CSI gRPC server
	// since it just runs the backup garbage collector
	if d.config.PluginType == "backupGC" {
		klog.Info("Running in backup garbage collector mode - no CSI gRPC server needed")
		// Use a channel to keep the process alive
		forever := make(chan struct{})
		<-forever
		return nil
	}

	// Initialize and start listening on grpc server for controller and agent plugin types
	s := NewNonBlockingGRPCServer(d.config.Endpoint, d.ids, d.cs, d.ns)

	s.Start()
	s.Wait()

	return nil
}
