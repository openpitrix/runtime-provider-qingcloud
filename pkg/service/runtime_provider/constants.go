// Copyright 2018 The OpenPitrix Authors. All rights reserved.
// Use of this source code is governed by a Apache license
// that can be found in the LICENSE file.

package runtime_provider

const (
	Provider       = "qingcloud"
	ProviderConfig = `
api_server: api.qingcloud.com
zone: .*
image_id: xenial4x64a
image_url: https://openpitrix.pek3a.qingstor.com/image/ubuntu.tar.gz
provider_type: vmbased
`
)

const (
	DefaultInstanceClass = 1
	DefaultVolumeClass   = 3

	DefaultLoginMode     = "passwd"
	DefaultLoginPassword = "p12cHANgepwD"

	DefaultUserDataType = "exec"

	ResourceTypeInstance   = "hp_instance"
	ResourceTypeCpu        = "hp_cpu"
	ResourceTypeGpu        = "gpu_passthrough"
	ResourceTypeMemory     = "hp_memory"
	ResourceTypeVolume     = "hpp_volume"
	ResourceTypeVolumeSize = "hpp_volume_size"
)
