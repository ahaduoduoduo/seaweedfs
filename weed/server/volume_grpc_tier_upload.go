package weed_server

import (
	"fmt"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// clampTierConcurrency bounds a per-request transfer concurrency so a direct
// gRPC caller cannot spawn an unbounded number of network workers.
const maxTierConcurrency = 1024

func clampTierConcurrency(n int) int {
	if n < 0 {
		return 0
	}
	if n > maxTierConcurrency {
		return maxTierConcurrency
	}
	return n
}

// VolumeTierMoveDatToRemote copy dat file to a remote tier
func (vs *VolumeServer) VolumeTierMoveDatToRemote(req *volume_server_pb.VolumeTierMoveDatToRemoteRequest, stream volume_server_pb.VolumeServer_VolumeTierMoveDatToRemoteServer) error {
	if err := vs.checkGrpcAdminAuth(stream.Context()); err != nil {
		return err
	}
	if err := vs.CheckMaintenanceMode(); err != nil {
		return err
	}

	// find existing volume
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return fmt.Errorf("volume %d not found", req.VolumeId)
	}

	// verify the collection
	if v.Collection != req.Collection {
		return fmt.Errorf("existing collection:%v unexpected input: %v", v.Collection, req.Collection)
	}

	// locate the disk file
	diskFile, ok := v.DataBackend.(*backend.DiskFile)
	if !ok {
		return nil // already copied to remove. fmt.Errorf("volume %d is not on local disk", req.VolumeId)
	}
	_, modTime, err := diskFile.GetStat()
	if err != nil {
		return fmt.Errorf("stat data file %s: %v", diskFile.Name(), err)
	}

	// check valid storage backend type
	backendStorage, found := backend.BackendStorages[req.DestinationBackendName]
	if !found {
		var keys []string
		for key := range backend.BackendStorages {
			keys = append(keys, key)
		}
		return fmt.Errorf("destination %s not found, supported: %v", req.DestinationBackendName, keys)
	}

	// OpenList's content-addressed upload is idempotent. Re-copying after a .vif
	// commit failure returns the same receipt without another provider upload.
	backendType, backendId := backend.BackendNameToTypeId(req.DestinationBackendName)
	for _, remoteFile := range v.GetVolumeInfo().GetFiles() {
		if remoteFile.BackendType == backendType && remoteFile.BackendId == backendId && backendType != "openlist" {
			return fmt.Errorf("destination %s already exists", req.DestinationBackendName)
		}
	}
	if backendType == "openlist" {
		if !v.IsReadOnly() {
			return fmt.Errorf("OpenList tier upload requires a read-only volume")
		}
		if err := diskFile.Sync(); err != nil {
			return fmt.Errorf("sync volume before tier upload: %w", err)
		}
	}

	startTime := time.Now()
	fn := func(progressed int64, percentage float32) error {
		now := time.Now()
		if now.Sub(startTime) < time.Second {
			return nil
		}
		startTime = now
		return stream.Send(&volume_server_pb.VolumeTierMoveDatToRemoteResponse{
			Processed:           progressed,
			ProcessedPercentage: percentage,
		})
	}

	// copy the data file
	key, size, err := backendStorage.CopyFile(diskFile.File, fn, clampTierConcurrency(int(req.Concurrency)))
	if err != nil {
		return fmt.Errorf("backend %s copy file %s: %v", req.DestinationBackendName, diskFile.Name(), err)
	}

	// save the remote file to volume tier info
	remote := &volume_server_pb.RemoteFile{
		BackendType:  backendType,
		BackendId:    backendId,
		Key:          key,
		Offset:       0,
		FileSize:     uint64(size),
		ModifiedTime: uint64(modTime.Unix()),
		Extension:    ".dat",
	}

	if err := v.CommitRemoteFile(remote); err != nil {
		return fmt.Errorf("volume %d failed to save remote file info: %v", v.Id, err)
	}

	if err := v.LoadRemoteFile(); err != nil {
		return fmt.Errorf("volume %d failed to load remote file: %v", v.Id, err)
	}

	if !req.KeepLocalDatFile {
		if err := os.Remove(v.FileName(".dat")); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remote tier committed but local cleanup failed: %w", err)
		}
	}

	return nil
}
