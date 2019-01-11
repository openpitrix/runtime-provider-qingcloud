// Copyright 2018 The OpenPitrix Authors. All rights reserved.
// Use of this source code is governed by a Apache license
// that can be found in the LICENSE file.

package runtime_provider

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	qcclient "github.com/yunify/qingcloud-sdk-go/client"
	qcconfig "github.com/yunify/qingcloud-sdk-go/config"
	qcservice "github.com/yunify/qingcloud-sdk-go/service"

	runtimeclient "openpitrix.io/openpitrix/pkg/client/runtime"
	"openpitrix.io/openpitrix/pkg/constants"
	"openpitrix.io/openpitrix/pkg/logger"
	"openpitrix.io/openpitrix/pkg/models"
	"openpitrix.io/openpitrix/pkg/pb"
	"openpitrix.io/openpitrix/pkg/plugins/vmbased"
	"openpitrix.io/openpitrix/pkg/util/funcutil"
	"openpitrix.io/openpitrix/pkg/util/jsonutil"
	"openpitrix.io/openpitrix/pkg/util/pbutil"
)

var MyProvider = constants.ProviderQingCloud

type ProviderHandler struct {
	vmbased.FrameHandler
}

func (p *ProviderHandler) initQingCloudService(ctx context.Context, runtimeUrl, runtimeCredential, zone string) (*qcservice.QingCloudService, error) {
	credential := new(vmbased.Credential)
	err := jsonutil.Decode([]byte(runtimeCredential), credential)
	if err != nil {
		logger.Error(ctx, "Parse [%s] credential failed: %+v", MyProvider, err)
		return nil, err
	}
	conf, err := qcconfig.New(credential.AccessKeyId, credential.SecretAccessKey)
	if err != nil {
		return nil, err
	}
	conf.Zone = zone
	if strings.HasPrefix(runtimeUrl, "https://") {
		runtimeUrl = strings.Split(runtimeUrl, "https://")[1]
	} else if strings.HasPrefix(runtimeUrl, "http://") {
		runtimeUrl = strings.Split(runtimeUrl, "http://")[1]
	}
	hostAndPort := strings.Split(strings.Split(runtimeUrl, "/")[0], ":")
	if len(hostAndPort) == 2 {
		conf.Port, err = strconv.Atoi(hostAndPort[1])
	}
	conf.Host = hostAndPort[0]
	if err != nil {
		logger.Error(ctx, "Parse [%s] runtimeUrl [%s] failed: %+v", MyProvider, runtimeUrl, err)
		return nil, err
	}
	return qcservice.Init(conf)
}

func (p *ProviderHandler) initService(ctx context.Context, runtimeId string) (*qcservice.QingCloudService, error) {
	runtime, err := runtimeclient.NewRuntime(ctx, runtimeId)
	if err != nil {
		return nil, err
	}
	return p.initQingCloudService(ctx, runtime.RuntimeUrl, runtime.RuntimeCredentialContent, runtime.Zone)
}

func (p *ProviderHandler) waitInstanceNetworkAndVolume(ctx context.Context, instanceService *qcservice.InstanceService, instanceId string, needVolume bool, timeout time.Duration, waitInterval time.Duration) (ins *qcservice.Instance, err error) {
	logger.Debug(ctx, "Waiting for IP address to be assigned and volume attached to Instance [%s]", instanceId)
	err = funcutil.WaitForSpecificOrError(func() (bool, error) {
		describeOutput, err := instanceService.DescribeInstances(
			&qcservice.DescribeInstancesInput{
				Instances: qcservice.StringSlice([]string{instanceId}),
			},
		)
		if err != nil {
			return false, err
		}

		describeRetCode := qcservice.IntValue(describeOutput.RetCode)
		if describeRetCode != 0 {
			return false, err
		}
		if len(describeOutput.InstanceSet) == 0 {
			return false, fmt.Errorf("instance with id [%s] not exist", instanceId)
		}
		instance := describeOutput.InstanceSet[0]
		if len(instance.VxNets) == 0 || instance.VxNets[0].PrivateIP == nil || *instance.VxNets[0].PrivateIP == "" {
			return false, nil
		}
		if needVolume {
			if len(instance.Volumes) == 0 || instance.Volumes[0].Device == nil || *instance.Volumes[0].Device == "" {
				return false, nil
			}
		}
		ins = instance
		logger.Debug(ctx, "Instance [%s] get IP address [%s]", instanceId, *ins.VxNets[0].PrivateIP)
		return true, nil
	}, timeout, waitInterval)
	return
}

func (p *ProviderHandler) RunInstances(ctx context.Context, task *models.Task) (*models.Task, error) {

	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	instance, err := models.NewInstance(task.Directive)
	if err != nil {
		return task, err
	}
	qingcloudService, err := p.initService(ctx, instance.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init %s api service failed: %+v", MyProvider, err)
		return task, err
	}

	instanceService, err := qingcloudService.Instance(qingcloudService.Config.Zone)
	if err != nil {
		logger.Error(ctx, "Init %s instance api service failed: %+v", MyProvider, err)
		return task, err
	}

	input := &qcservice.RunInstancesInput{
		ImageID:       qcservice.String(instance.ImageId),
		CPU:           qcservice.Int(instance.Cpu),
		Memory:        qcservice.Int(instance.Memory),
		InstanceName:  qcservice.String(instance.Name),
		InstanceClass: qcservice.Int(DefaultInstanceClass),
		VxNets:        qcservice.StringSlice([]string{instance.Subnet}),
		LoginMode:     qcservice.String(DefaultLoginMode),
		LoginPasswd:   qcservice.String(DefaultLoginPassword),
		NeedUserdata:  qcservice.Int(instance.NeedUserData),
		Hostname:      qcservice.String(instance.Hostname),
		Gpu:           qcservice.Int(instance.Gpu),
	}
	if instance.VolumeId != "" {
		input.Volumes = qcservice.StringSlice([]string{instance.VolumeId})
	}
	if instance.UserdataFile != "" {
		input.UserdataFile = qcservice.String(instance.UserdataFile)
	}
	if instance.UserDataValue != "" {
		input.UserdataValue = qcservice.String(instance.UserDataValue)
		input.UserdataType = qcservice.String(DefaultUserDataType)
	}
	logger.Debug(ctx, "RunInstances with input: %s", jsonutil.ToString(input))
	output, err := instanceService.RunInstances(input)
	if err != nil {
		logger.Error(ctx, "Send RunInstances to %s failed: %+v", MyProvider, err)
		return task, err
	}

	retCode := qcservice.IntValue(output.RetCode)
	if retCode != 0 {
		message := qcservice.StringValue(output.Message)
		logger.Error(ctx, "Send RunInstances to %s failed with return code [%d], message [%s]",
			MyProvider, retCode, message)
		return task, fmt.Errorf("send RunInstances to %s failed: %s", MyProvider, message)
	}

	if len(output.Instances) == 0 {
		logger.Error(ctx, "Send RunInstances to %s failed with 0 output instances", MyProvider)
		return task, fmt.Errorf("send RunInstances to %s failed with 0 output instances", MyProvider)
	}

	instance.InstanceId = qcservice.StringValue(output.Instances[0])
	instance.TargetJobId = qcservice.StringValue(output.JobID)

	// write back
	task.Directive = jsonutil.ToString(instance)

	return task, nil
}

func (p *ProviderHandler) StopInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	instance, err := models.NewInstance(task.Directive)
	if err != nil {
		return task, err
	}
	if instance.InstanceId == "" {
		logger.Warn(ctx, "Skip task without instance")
		return task, nil
	}
	qingcloudService, err := p.initService(ctx, instance.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init %s api service failed: %+v", MyProvider, err)
		return task, err
	}

	instanceService, err := qingcloudService.Instance(qingcloudService.Config.Zone)
	if err != nil {
		logger.Error(ctx, "Init %s instance api service failed: %+v", MyProvider, err)
		return task, err
	}

	describeOutput, err := instanceService.DescribeInstances(
		&qcservice.DescribeInstancesInput{
			Instances: qcservice.StringSlice([]string{instance.InstanceId}),
		},
	)
	if err != nil {
		logger.Error(ctx, "Send DescribeInstances to %s failed: %+v", MyProvider, err)
		return task, err
	}

	describeRetCode := qcservice.IntValue(describeOutput.RetCode)
	if describeRetCode != 0 {
		message := qcservice.StringValue(describeOutput.Message)
		logger.Error(ctx, "Send DescribeInstances to %s failed with return code [%d], message [%s]",
			MyProvider, describeRetCode, message)
		return task, fmt.Errorf("send DescribeInstances to %s failed: %s", MyProvider, message)
	}
	if len(describeOutput.InstanceSet) == 0 {
		return task, fmt.Errorf("Instance with id [%s] not exist", instance.InstanceId)
	}

	status := qcservice.StringValue(describeOutput.InstanceSet[0].Status)

	if status == constants.StatusStopped {
		logger.Warn(ctx, "Instance [%s] has already been [%s], do nothing", instance.InstanceId, status)
		return task, nil
	}

	output, err := instanceService.StopInstances(
		&qcservice.StopInstancesInput{
			Instances: qcservice.StringSlice([]string{instance.InstanceId}),
		},
	)
	if err != nil {
		logger.Error(ctx, "Send StopInstances to %s failed: %+v", MyProvider, err)
		return task, err
	}

	retCode := qcservice.IntValue(output.RetCode)
	if retCode != 0 {
		message := qcservice.StringValue(output.Message)
		logger.Error(ctx, "Send StopInstances to %s failed with return code [%d], message [%s]",
			MyProvider, retCode, message)
		return task, fmt.Errorf("send StopInstances to %s failed: %s", MyProvider, message)
	}
	instance.TargetJobId = qcservice.StringValue(output.JobID)

	// write back
	task.Directive = jsonutil.ToString(instance)

	return task, nil
}

func (p *ProviderHandler) StartInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	instance, err := models.NewInstance(task.Directive)
	if err != nil {
		return task, err
	}
	if instance.InstanceId == "" {
		logger.Warn(ctx, "Skip task without instance id")
		return task, nil
	}
	qingcloudService, err := p.initService(ctx, instance.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init %s api service failed: %+v", MyProvider, err)
		return task, err
	}

	instanceService, err := qingcloudService.Instance(qingcloudService.Config.Zone)
	if err != nil {
		logger.Error(ctx, "Init %s instance api service failed: %+v", MyProvider, err)
		return task, err
	}

	describeOutput, err := instanceService.DescribeInstances(
		&qcservice.DescribeInstancesInput{
			Instances: qcservice.StringSlice([]string{instance.InstanceId}),
		},
	)
	if err != nil {
		logger.Error(ctx, "Send DescribeInstances to %s failed: %+v", MyProvider, err)
		return task, err
	}

	describeRetCode := qcservice.IntValue(describeOutput.RetCode)
	if describeRetCode != 0 {
		message := qcservice.StringValue(describeOutput.Message)
		logger.Error(ctx, "Send DescribeInstances to %s failed with return code [%d], message [%s]",
			MyProvider, describeRetCode, message)
		return task, fmt.Errorf("send DescribeInstances to %s failed: %s", MyProvider, message)
	}
	if len(describeOutput.InstanceSet) == 0 {
		return task, fmt.Errorf("instance id [%s] not exist", instance.InstanceId)
	}

	status := qcservice.StringValue(describeOutput.InstanceSet[0].Status)

	if status == constants.StatusRunning {
		logger.Warn(ctx, "Instance [%s] has already been [%s], do nothing", instance.InstanceId, status)
		return task, nil
	}

	output, err := instanceService.StartInstances(
		&qcservice.StartInstancesInput{
			Instances: qcservice.StringSlice([]string{instance.InstanceId}),
		},
	)
	if err != nil {
		logger.Error(ctx, "Send StartInstances to %s failed: %+v", MyProvider, err)
		return task, err
	}

	retCode := qcservice.IntValue(output.RetCode)
	if retCode != 0 {
		message := qcservice.StringValue(output.Message)
		logger.Error(ctx, "Send StartInstances to %s failed with return code [%d], message [%s]",
			MyProvider, retCode, message)
		return task, fmt.Errorf("send StartInstances to %s failed: %s", MyProvider, message)
	}
	instance.TargetJobId = qcservice.StringValue(output.JobID)

	// write back
	task.Directive = jsonutil.ToString(instance)

	return task, nil
}

func (p *ProviderHandler) DeleteInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	instance, err := models.NewInstance(task.Directive)
	if err != nil {
		return task, err
	}
	if instance.InstanceId == "" {
		logger.Warn(ctx, "Skip task without instance id")
		return task, nil
	}

	qingcloudService, err := p.initService(ctx, instance.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init %s api service failed: %+v", MyProvider, err)
		return task, err
	}

	instanceService, err := qingcloudService.Instance(qingcloudService.Config.Zone)
	if err != nil {
		logger.Error(ctx, "Init %s instance api service failed: %+v", MyProvider, err)
		return task, err
	}

	describeOutput, err := instanceService.DescribeInstances(
		&qcservice.DescribeInstancesInput{
			Instances: qcservice.StringSlice([]string{instance.InstanceId}),
		},
	)
	if err != nil {
		logger.Error(ctx, "Send DescribeInstances to %s failed: %+v", MyProvider, err)
		return task, err
	}

	describeRetCode := qcservice.IntValue(describeOutput.RetCode)
	if describeRetCode != 0 {
		message := qcservice.StringValue(describeOutput.Message)
		logger.Error(ctx, "Send DescribeInstances to %s failed with return code [%d], message [%s]",
			MyProvider, describeRetCode, message)
		return task, fmt.Errorf("send DescribeInstances to %s failed: %s", MyProvider, message)
	}
	if len(describeOutput.InstanceSet) == 0 {
		return task, fmt.Errorf("instance id [%s] not exist", instance.InstanceId)
	}

	status := qcservice.StringValue(describeOutput.InstanceSet[0].Status)

	if status == constants.StatusDeleted || status == constants.StatusCeased {
		logger.Warn(ctx, "Instance [%s] has already been [%s], do nothing", instance.InstanceId, status)
		return task, nil
	}

	output, err := instanceService.TerminateInstances(
		&qcservice.TerminateInstancesInput{
			Instances: qcservice.StringSlice([]string{instance.InstanceId}),
		},
	)
	if err != nil {
		logger.Error(ctx, "Send TerminateInstances to %s failed: %+v", MyProvider, err)
		return task, err
	}

	retCode := qcservice.IntValue(output.RetCode)
	if retCode != 0 {
		message := qcservice.StringValue(output.Message)
		logger.Error(ctx, "Send TerminateInstances to %s failed with return code [%d], message [%s]",
			MyProvider, retCode, message)
		return task, fmt.Errorf("send TerminateInstances to %s failed: %s", MyProvider, message)
	}
	instance.TargetJobId = qcservice.StringValue(output.JobID)

	// write back
	task.Directive = jsonutil.ToString(instance)
	return task, nil
}

func (p *ProviderHandler) ResizeInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	instance, err := models.NewInstance(task.Directive)
	if err != nil {
		return task, err
	}
	if instance.InstanceId == "" {
		logger.Warn(ctx, "Skip task without instance id")
		return task, nil
	}
	qingcloudService, err := p.initService(ctx, instance.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init %s api service failed: %+v", MyProvider, err)
		return task, err
	}

	instanceService, err := qingcloudService.Instance(qingcloudService.Config.Zone)
	if err != nil {
		logger.Error(ctx, "Init %s instance api service failed: %+v", MyProvider, err)
		return task, err
	}

	describeOutput, err := instanceService.DescribeInstances(
		&qcservice.DescribeInstancesInput{
			Instances: qcservice.StringSlice([]string{instance.InstanceId}),
		},
	)
	if err != nil {
		logger.Error(ctx, "Send DescribeInstances to %s failed: %+v", MyProvider, err)
		return task, err
	}

	describeRetCode := qcservice.IntValue(describeOutput.RetCode)
	if describeRetCode != 0 {
		message := qcservice.StringValue(describeOutput.Message)
		logger.Error(ctx, "Send DescribeInstances to %s failed with return code [%d], message [%s]",
			MyProvider, describeRetCode, message)
		return task, fmt.Errorf("send DescribeInstances to %s failed: %s", MyProvider, message)
	}
	if len(describeOutput.InstanceSet) == 0 {
		return task, fmt.Errorf("instance id [%s] not exist", instance.InstanceId)
	}

	status := qcservice.StringValue(describeOutput.InstanceSet[0].Status)

	if status != constants.StatusStopped {
		logger.Warn(ctx, "Instance [%s] is in status [%s], can not resize", instance.InstanceId, status)
		return task, fmt.Errorf("instance [%s] is in status [%s], can not resize", instance.InstanceId, status)
	}

	output, err := instanceService.ResizeInstances(
		&qcservice.ResizeInstancesInput{
			Instances: qcservice.StringSlice([]string{instance.InstanceId}),
			CPU:       qcservice.Int(instance.Cpu),
			Memory:    qcservice.Int(instance.Memory),
		},
	)
	if err != nil {
		logger.Error(ctx, "Send ResizeInstances to %s failed: %+v", MyProvider, err)
		return task, err
	}

	retCode := qcservice.IntValue(output.RetCode)
	if retCode != 0 {
		message := qcservice.StringValue(output.Message)
		logger.Error(ctx, "Send ResizeInstances to %s failed with return code [%d], message [%s]",
			MyProvider, retCode, message)
		return task, fmt.Errorf("send ResizeInstances to %s failed: %s", MyProvider, message)
	}
	instance.TargetJobId = qcservice.StringValue(output.JobID)

	// write back
	task.Directive = jsonutil.ToString(instance)

	return task, nil
}

func (p *ProviderHandler) CreateVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	volume, err := models.NewVolume(task.Directive)
	if err != nil {
		return task, err
	}
	qingcloudService, err := p.initService(ctx, volume.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init %s api service failed: %+v", MyProvider, err)
		return task, err
	}

	volumeService, err := qingcloudService.Volume(qingcloudService.Config.Zone)
	if err != nil {
		logger.Error(ctx, "Init %s volume api service failed: %+v", MyProvider, err)
		return task, err
	}

	output, err := volumeService.CreateVolumes(
		&qcservice.CreateVolumesInput{
			Size:       qcservice.Int(volume.Size),
			VolumeName: qcservice.String(volume.Name),
			VolumeType: qcservice.Int(DefaultVolumeClass),
		},
	)
	if err != nil {
		logger.Error(ctx, "Send CreateVolumes to %s failed: %+v", MyProvider, err)
		return task, err
	}

	retCode := qcservice.IntValue(output.RetCode)
	if retCode != 0 {
		message := qcservice.StringValue(output.Message)
		logger.Error(ctx, "Send CreateVolumes to %s failed with return code [%d], message [%s]",
			MyProvider, retCode, message)
		return task, fmt.Errorf("send CreateVolumes to %s failed: %s", MyProvider, message)
	}
	volume.VolumeId = qcservice.StringValue(output.Volumes[0])
	volume.TargetJobId = qcservice.StringValue(output.JobID)

	// write back
	task.Directive = jsonutil.ToString(volume)

	return task, nil
}

func (p *ProviderHandler) DetachVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}

	volume, err := models.NewVolume(task.Directive)
	if err != nil {
		return task, err
	}

	qingcloudService, err := p.initService(ctx, volume.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init %s api service failed: %+v", MyProvider, err)
		return task, err
	}

	volumeService, err := qingcloudService.Volume(qingcloudService.Config.Zone)
	if err != nil {
		logger.Error(ctx, "Init %s volume api service failed: %+v", MyProvider, err)
		return task, err
	}

	describeOutput, err := volumeService.DescribeVolumes(
		&qcservice.DescribeVolumesInput{
			Volumes: qcservice.StringSlice([]string{volume.VolumeId}),
		},
	)
	if err != nil {
		logger.Error(ctx, "Send DescribeVolumes to %s failed: %+v", MyProvider, err)
		return task, err
	}

	describeRetCode := qcservice.IntValue(describeOutput.RetCode)
	if describeRetCode != 0 {
		message := qcservice.StringValue(describeOutput.Message)
		logger.Error(ctx, "Send DescribeVolumes to %s failed with return code [%d], message [%s]",
			MyProvider, describeRetCode, message)
		return task, fmt.Errorf("send DescribeVolumes to %s failed: %s", MyProvider, message)
	}
	if len(describeOutput.VolumeSet) == 0 {
		logger.Error(ctx, "Volume with id [%s] not exist", volume.VolumeId)
		return task, fmt.Errorf("volume with id [%s] not exist", volume.VolumeId)
	}

	status := qcservice.StringValue(describeOutput.VolumeSet[0].Status)
	if status == constants.StatusDeleted || status == constants.StatusCeased {
		logger.Warn(ctx, "Volume [%s] has already been [%s], do nothing", volume.VolumeId, status)
		return task, nil
	}

	if describeOutput.VolumeSet[0].Instance == nil || len(qcservice.StringValue(describeOutput.VolumeSet[0].Instance.InstanceID)) == 0 {
		logger.Warn(ctx, "Volume [%s] has not been attached, do nothing", volume.VolumeId)
		return task, nil
	}

	output, err := volumeService.DetachVolumes(
		&qcservice.DetachVolumesInput{
			Instance: qcservice.String(volume.InstanceId),
			Volumes:  qcservice.StringSlice([]string{volume.VolumeId}),
		},
	)
	if err != nil {
		logger.Error(ctx, "Send DetachVolumes to %s failed: %+v", MyProvider, err)
		return task, err
	}

	retCode := qcservice.IntValue(output.RetCode)
	if retCode != 0 {
		message := qcservice.StringValue(output.Message)
		logger.Error(ctx, "Send DetachVolumes to %s failed with return code [%d], message [%s]",
			MyProvider, retCode, message)
		return task, fmt.Errorf("send DetachVolumes to %s failed: %s", MyProvider, message)
	}
	volume.TargetJobId = qcservice.StringValue(output.JobID)

	// write back
	task.Directive = jsonutil.ToString(volume)

	return task, nil
}

func (p *ProviderHandler) AttachVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}

	volume, err := models.NewVolume(task.Directive)
	if err != nil {
		return task, err
	}

	qingcloudService, err := p.initService(ctx, volume.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init %s api service failed: %+v", MyProvider, err)
		return task, err
	}

	volumeService, err := qingcloudService.Volume(qingcloudService.Config.Zone)
	if err != nil {
		logger.Error(ctx, "Init %s volume api service failed: %+v", MyProvider, err)
		return task, err
	}

	describeOutput, err := volumeService.DescribeVolumes(
		&qcservice.DescribeVolumesInput{
			Volumes: qcservice.StringSlice([]string{volume.VolumeId}),
		},
	)
	if err != nil {
		logger.Error(ctx, "Send DescribeVolumes to %s failed: %+v", MyProvider, err)
		return task, err
	}

	describeRetCode := qcservice.IntValue(describeOutput.RetCode)
	if describeRetCode != 0 {
		message := qcservice.StringValue(describeOutput.Message)
		logger.Error(ctx, "Send DescribeVolumes to %s failed with return code [%d], message [%s]",
			MyProvider, describeRetCode, message)
		return task, fmt.Errorf("send DescribeVolumes to %s failed: %s", MyProvider, message)
	}
	if len(describeOutput.VolumeSet) == 0 {
		logger.Error(ctx, "Volume with id [%s] not exist", volume.VolumeId)
		return task, fmt.Errorf("volume with id [%s] not exist", volume.VolumeId)
	}

	if describeOutput.VolumeSet[0].Instance != nil && qcservice.StringValue(describeOutput.VolumeSet[0].Instance.InstanceID) == volume.InstanceId {
		logger.Warn(ctx, "Volume [%s] has already been attached to instance [%s], do nothing", volume.VolumeId, volume.InstanceId)
		return task, nil
	}

	output, err := volumeService.AttachVolumes(
		&qcservice.AttachVolumesInput{
			Instance: qcservice.String(volume.InstanceId),
			Volumes:  qcservice.StringSlice([]string{volume.VolumeId}),
		},
	)
	if err != nil {
		logger.Error(ctx, "Send AttachVolumes to %s failed: %+v", MyProvider, err)
		return task, err
	}

	retCode := qcservice.IntValue(output.RetCode)
	if retCode != 0 {
		message := qcservice.StringValue(output.Message)
		logger.Error(ctx, "Send AttachVolumes to %s failed with return code [%d], message [%s]",
			MyProvider, retCode, message)
		return task, fmt.Errorf("send AttachVolumes to %s failed: %s", MyProvider, message)
	}
	volume.TargetJobId = qcservice.StringValue(output.JobID)

	// write back
	task.Directive = jsonutil.ToString(volume)

	return task, nil
}

func (p *ProviderHandler) DeleteVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}

	volume, err := models.NewVolume(task.Directive)
	if err != nil {
		return task, err
	}
	if volume.VolumeId == "" {
		logger.Warn(ctx, "Skip task without volume")
		return task, nil
	}
	qingcloudService, err := p.initService(ctx, volume.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init %s api service failed: %+v", MyProvider, err)
		return task, err
	}

	volumeService, err := qingcloudService.Volume(qingcloudService.Config.Zone)
	if err != nil {
		logger.Error(ctx, "Init %s volume api service failed: %+v", MyProvider, err)
		return task, err
	}

	describeOutput, err := volumeService.DescribeVolumes(
		&qcservice.DescribeVolumesInput{
			Volumes: qcservice.StringSlice([]string{volume.VolumeId}),
		},
	)
	if err != nil {
		logger.Error(ctx, "Send DescribeVolumes to %s failed: %+v", MyProvider, err)
		return task, err
	}

	describeRetCode := qcservice.IntValue(describeOutput.RetCode)
	if describeRetCode != 0 {
		message := qcservice.StringValue(describeOutput.Message)
		logger.Error(ctx, "Send DescribeVolumes to %s failed with return code [%d], message [%s]",
			MyProvider, describeRetCode, message)
		return task, fmt.Errorf("send DescribeVolumes to %s failed: %s", MyProvider, message)
	}
	if len(describeOutput.VolumeSet) == 0 {
		return task, fmt.Errorf("volume with id [%s] not exist", volume.VolumeId)
	}

	status := qcservice.StringValue(describeOutput.VolumeSet[0].Status)

	if status == constants.StatusDeleted || status == constants.StatusCeased {
		logger.Warn(ctx, "Volume [%s] has already been [%s], do nothing", volume.VolumeId, status)
		return task, nil
	}

	output, err := volumeService.DeleteVolumes(
		&qcservice.DeleteVolumesInput{
			Volumes: qcservice.StringSlice([]string{volume.VolumeId}),
		},
	)
	if err != nil {
		logger.Error(ctx, "Send DeleteVolumes to %s failed: %+v", MyProvider, err)
		return task, err
	}

	retCode := qcservice.IntValue(output.RetCode)
	if retCode != 0 {
		message := qcservice.StringValue(output.Message)
		logger.Error(ctx, "Send DeleteVolumes to %s failed with return code [%d], message [%s]",
			MyProvider, retCode, message)
		return task, fmt.Errorf("send DeleteVolumes to %s failed: %s", MyProvider, message)
	}
	volume.TargetJobId = qcservice.StringValue(output.JobID)

	// write back
	task.Directive = jsonutil.ToString(volume)

	return task, nil
}

func (p *ProviderHandler) ResizeVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}

	volume, err := models.NewVolume(task.Directive)
	if err != nil {
		return task, err
	}
	if volume.VolumeId == "" {
		logger.Warn(ctx, "Skip task without volume")
		return task, nil
	}
	qingcloudService, err := p.initService(ctx, volume.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init %s api service failed: %+v", MyProvider, err)
		return task, err
	}

	volumeService, err := qingcloudService.Volume(qingcloudService.Config.Zone)
	if err != nil {
		logger.Error(ctx, "Init %s volume api service failed: %+v", MyProvider, err)
		return task, err
	}

	describeOutput, err := volumeService.DescribeVolumes(
		&qcservice.DescribeVolumesInput{
			Volumes: qcservice.StringSlice([]string{volume.VolumeId}),
		},
	)
	if err != nil {
		logger.Error(ctx, "Send DescribeVolumes to %s failed: %+v", MyProvider, err)
		return task, err
	}

	describeRetCode := qcservice.IntValue(describeOutput.RetCode)
	if describeRetCode != 0 {
		message := qcservice.StringValue(describeOutput.Message)
		logger.Error(ctx, "Send DescribeVolumes to %s failed with return code [%d], message [%s]",
			MyProvider, describeRetCode, message)
		return task, fmt.Errorf("send DescribeVolumes to %s failed: %s", MyProvider, message)
	}
	if len(describeOutput.VolumeSet) == 0 {
		return task, fmt.Errorf("volume with id [%s] not exist", volume.VolumeId)
	}

	status := qcservice.StringValue(describeOutput.VolumeSet[0].Status)

	if status != constants.StatusAvailable {
		logger.Warn(ctx, "Volume [%s] is in status [%s], can not resize.", volume.VolumeId, status)
		return task, fmt.Errorf("volume [%s] is in status [%s], can not resize", volume.VolumeId, status)
	}

	output, err := volumeService.ResizeVolumes(
		&qcservice.ResizeVolumesInput{
			Volumes: qcservice.StringSlice([]string{volume.VolumeId}),
			Size:    qcservice.Int(volume.Size),
		},
	)
	if err != nil {
		logger.Error(ctx, "Send ResizeVolumes to %s failed: %+v", MyProvider, err)
		return task, err
	}

	retCode := qcservice.IntValue(output.RetCode)
	if retCode != 0 {
		message := qcservice.StringValue(output.Message)
		logger.Error(ctx, "Send ResizeVolumes to %s failed with return code [%d], message [%s]",
			MyProvider, retCode, message)
		return task, fmt.Errorf("send ResizeVolumes to %s failed: %s", MyProvider, message)
	}
	volume.TargetJobId = qcservice.StringValue(output.JobID)

	// write back
	task.Directive = jsonutil.ToString(volume)

	return task, nil
}

func (p *ProviderHandler) WaitRunInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	instance, err := models.NewInstance(task.Directive)
	if err != nil {
		return task, err
	}
	if instance.TargetJobId == "" {
		logger.Warn(ctx, "Skip task without target job id")
		return task, nil
	}

	qingcloudService, err := p.initService(ctx, instance.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init %s api service failed: %+v", MyProvider, err)
		return task, err
	}

	jobService, err := qingcloudService.Job(qingcloudService.Config.Zone)
	if err != nil {
		logger.Error(ctx, "Init %s job api service failed: %+v", MyProvider, err)
		return task, err
	}

	err = qcclient.WaitJob(jobService, instance.TargetJobId, task.GetTimeout(constants.WaitTaskTimeout),
		constants.WaitTaskInterval)
	if err != nil {
		logger.Error(ctx, "Wait %s job [%s] failed: %+v", MyProvider, instance.TargetJobId, err)
		return task, err
	}

	instanceService, err := qingcloudService.Instance(qingcloudService.Config.Zone)
	if err != nil {
		logger.Error(ctx, "Init %s instance api service failed: %+v", MyProvider, err)
		return task, err
	}

	needVolume := false
	if instance.VolumeId != "" {
		needVolume = true
	}

	output, err := p.waitInstanceNetworkAndVolume(ctx, instanceService, instance.InstanceId, needVolume,
		task.GetTimeout(constants.WaitTaskTimeout), constants.WaitTaskInterval)
	if err != nil {
		logger.Error(ctx, "Wait %s instance [%s] network failed: %+v", MyProvider, instance.InstanceId, err)
		return task, err
	}

	instance.PrivateIp = qcservice.StringValue(output.VxNets[0].PrivateIP)
	if len(output.Volumes) > 0 {
		instance.Device = qcservice.StringValue(output.Volumes[0].Device)
	}

	// write back
	task.Directive = jsonutil.ToString(instance)

	logger.Debug(ctx, "WaitRunInstances task [%s] directive: %s", task.TaskId, task.Directive)

	return task, nil
}

func (p *ProviderHandler) WaitInstanceTask(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	instance, err := models.NewInstance(task.Directive)
	if err != nil {
		return task, err
	}
	if instance.TargetJobId == "" {
		logger.Warn(ctx, "Skip task without target job id")
		return task, nil
	}
	qingcloudService, err := p.initService(ctx, instance.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init %s api service failed: %+v", MyProvider, err)
		return task, err
	}

	jobService, err := qingcloudService.Job(qingcloudService.Config.Zone)
	if err != nil {
		logger.Error(ctx, "Init %s job api service failed: %+v", MyProvider, err)
		return task, err
	}

	err = qcclient.WaitJob(jobService, instance.TargetJobId, task.GetTimeout(constants.WaitTaskTimeout),
		constants.WaitTaskInterval)
	if err != nil {
		logger.Error(ctx, "Wait %s job [%s] failed: %+v", MyProvider, instance.TargetJobId, err)
		return task, err
	}

	return task, nil
}

func (p *ProviderHandler) WaitVolumeTask(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	volume, err := models.NewVolume(task.Directive)
	if err != nil {
		return task, err
	}
	if volume.TargetJobId == "" {
		logger.Warn(ctx, "Skip task without target job id")
		return task, nil
	}
	qingcloudService, err := p.initService(ctx, volume.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init %s api service failed: %+v", MyProvider, err)
		return task, err
	}

	jobService, err := qingcloudService.Job(qingcloudService.Config.Zone)
	if err != nil {
		logger.Error(ctx, "Init %s volume api service failed: %+v", MyProvider, err)
		return task, err
	}

	err = qcclient.WaitJob(jobService, volume.TargetJobId, task.GetTimeout(constants.WaitTaskTimeout),
		constants.WaitTaskInterval)
	if err != nil {
		logger.Error(ctx, "Wait %s volume [%s] failed: %+v", MyProvider, volume.TargetJobId, err)
		return task, err
	}

	return task, nil
}

func (p *ProviderHandler) WaitStopInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	return p.WaitInstanceTask(ctx, task)
}

func (p *ProviderHandler) WaitStartInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	return p.WaitInstanceTask(ctx, task)
}

func (p *ProviderHandler) WaitDeleteInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	return p.WaitInstanceTask(ctx, task)
}

func (p *ProviderHandler) WaitResizeInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	return p.WaitInstanceTask(ctx, task)
}

func (p *ProviderHandler) WaitCreateVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	return p.WaitVolumeTask(ctx, task)
}

func (p *ProviderHandler) WaitAttachVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	return p.WaitVolumeTask(ctx, task)
}

func (p *ProviderHandler) WaitDetachVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	return p.WaitVolumeTask(ctx, task)
}

func (p *ProviderHandler) WaitDeleteVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	return p.WaitVolumeTask(ctx, task)
}

func (p *ProviderHandler) WaitResizeVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	return p.WaitVolumeTask(ctx, task)
}

func (p *ProviderHandler) DescribeSubnets(ctx context.Context, req *pb.DescribeSubnetsRequest) (*pb.DescribeSubnetsResponse, error) {
	qingcloudService, err := p.initService(ctx, req.GetRuntimeId().GetValue())
	if err != nil {
		logger.Error(ctx, "Init %s api service failed: %+v", MyProvider, err)
		return nil, err
	}

	vxnetService, err := qingcloudService.VxNet(qingcloudService.Config.Zone)
	if err != nil {
		logger.Error(ctx, "Init %s vxnet api service failed: %+v", MyProvider, err)
		return nil, err
	}

	input := new(qcservice.DescribeVxNetsInput)
	input.Verbose = qcservice.Int(1)
	if len(req.GetSubnetId()) > 0 {
		input.VxNets = qcservice.StringSlice(req.GetSubnetId())
	}
	if req.GetLimit() > 0 {
		input.Limit = qcservice.Int(int(req.GetLimit()))
	}
	if req.GetOffset() > 0 {
		input.Offset = qcservice.Int(int(req.GetOffset()))
	}
	if req.GetSubnetType().GetValue() > 0 {
		input.VxNetType = qcservice.Int(int(req.GetSubnetType().GetValue()))
	}

	output, err := vxnetService.DescribeVxNets(input)
	if err != nil {
		logger.Error(ctx, "DescribeVxNets to %s failed: %+v", MyProvider, err)
		return nil, err
	}

	retCode := qcservice.IntValue(output.RetCode)
	if retCode != 0 {
		message := qcservice.StringValue(output.Message)
		logger.Error(ctx, "Send DescribeVxNets to %s failed with return code [%d], message [%s]",
			MyProvider, retCode, message)
		return nil, fmt.Errorf("send DescribeVxNets to %s failed: %s", MyProvider, message)
	}

	if len(output.VxNetSet) == 0 {
		logger.Error(ctx, "Send DescribeVxNets to %s failed with 0 output subnets", MyProvider)
		return nil, fmt.Errorf("send DescribeVxNets to %s failed with 0 output subnets", MyProvider)
	}

	response := new(pb.DescribeSubnetsResponse)

	for _, vxnet := range output.VxNetSet {
		if vxnet.Router != nil && vxnet.VpcRouterID != nil && qcservice.StringValue(vxnet.VpcRouterID) != "" {
			vpc, err := p.DescribeVpc(ctx, req.GetRuntimeId().GetValue(), qcservice.StringValue(vxnet.VpcRouterID))
			if err != nil {
				return nil, err
			}
			if vpc.Eip != nil && vpc.Eip.Addr != "" {
				subnet := &pb.Subnet{
					SubnetId:    pbutil.ToProtoString(qcservice.StringValue(vxnet.VxNetID)),
					Name:        pbutil.ToProtoString(qcservice.StringValue(vxnet.VxNetName)),
					CreateTime:  pbutil.ToProtoTimestamp(qcservice.TimeValue(vxnet.CreateTime)),
					Description: pbutil.ToProtoString(qcservice.StringValue(vxnet.Description)),
					InstanceId:  qcservice.StringValueSlice(vxnet.InstanceIDs),
					VpcId:       pbutil.ToProtoString(qcservice.StringValue(vxnet.VpcRouterID)),
					SubnetType:  pbutil.ToProtoUInt32(uint32(qcservice.IntValue(vxnet.VxNetType))),
				}
				response.SubnetSet = append(response.SubnetSet, subnet)
			}
		}
	}

	response.TotalCount = uint32(len(response.SubnetSet))

	return response, nil
}

func (p *ProviderHandler) CheckResourceQuotas(ctx context.Context, clusterWrapper *models.ClusterWrapper) error {
	roleCount := make(map[string]int)
	for _, clusterNode := range clusterWrapper.ClusterNodesWithKeyPairs {
		role := clusterNode.Role
		_, isExist := roleCount[role]
		if isExist {
			roleCount[role] = roleCount[role] + 1
		} else {
			roleCount[role] = 1
		}
	}

	needQuotas := models.NewQuotas()
	needQuotas.Instance.Name = ResourceTypeInstance
	needQuotas.Cpu.Name = ResourceTypeCpu
	needQuotas.Gpu.Name = ResourceTypeGpu
	needQuotas.Memory.Name = ResourceTypeMemory
	needQuotas.Volume.Name = ResourceTypeVolume
	needQuotas.VolumeSize.Name = ResourceTypeVolumeSize
	for role, count := range roleCount {
		clusterRole := clusterWrapper.ClusterRoles[role]
		needQuotas.Instance.Count += count
		needQuotas.Cpu.Count += int(clusterRole.Cpu) * count
		needQuotas.Gpu.Count += int(clusterRole.Gpu) * count
		needQuotas.Memory.Count += int(clusterRole.Memory) * count
		needQuotas.Volume.Count += count
		needQuotas.VolumeSize.Count += int(clusterRole.StorageSize) * count
	}

	qingcloudService, err := p.initService(ctx, clusterWrapper.Cluster.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init %s api service failed: %+v", MyProvider, err)
		return err
	}

	resourceTypes := []string{ResourceTypeInstance, ResourceTypeCpu, ResourceTypeGpu, ResourceTypeMemory,
		ResourceTypeVolume, ResourceTypeVolumeSize}
	var qcResourceTypes []*string

	for _, resourceType := range resourceTypes {
		qcResourceTypes = append(qcResourceTypes, qcservice.String(resourceType))
	}

	miscService, err := qingcloudService.Misc()
	if err != nil {
		logger.Error(ctx, "Init %s misc api service failed: %+v", MyProvider, err)
		return err
	}
	output, err := miscService.GetQuotaLeft(&qcservice.GetQuotaLeftInput{
		ResourceTypes: qcResourceTypes,
		Zone:          qcservice.String(qingcloudService.Config.Zone),
	})
	if err != nil {
		logger.Error(ctx, "GetQuotaLeft to %s failed: %+v", MyProvider, err)
		return err
	}

	retCode := qcservice.IntValue(output.RetCode)
	if retCode != 0 {
		message := qcservice.StringValue(output.Message)
		logger.Error(ctx, "Send GetQuotaLeft to %s failed with return code [%d], message [%s]",
			MyProvider, retCode, message)
		return fmt.Errorf("send GetQuotaLeft to %s failed: %s", MyProvider, message)
	}

	leftQuotas := models.NewQuotas()
	for _, quotaLeftSet := range output.QuotaLeftSet {
		switch qcservice.StringValue(quotaLeftSet.ResourceType) {
		case ResourceTypeInstance:
			leftQuotas.Instance.Name = ResourceTypeInstance
			leftQuotas.Instance.Count = qcservice.IntValue(quotaLeftSet.Left)
		case ResourceTypeCpu:
			leftQuotas.Cpu.Name = ResourceTypeCpu
			leftQuotas.Cpu.Count = qcservice.IntValue(quotaLeftSet.Left)
		case ResourceTypeGpu:
			leftQuotas.Gpu.Name = ResourceTypeGpu
			leftQuotas.Gpu.Count = qcservice.IntValue(quotaLeftSet.Left)
		case ResourceTypeMemory:
			leftQuotas.Memory.Name = ResourceTypeMemory
			leftQuotas.Memory.Count = qcservice.IntValue(quotaLeftSet.Left)
		case ResourceTypeVolume:
			leftQuotas.Volume.Name = ResourceTypeVolume
			leftQuotas.Volume.Count = qcservice.IntValue(quotaLeftSet.Left)
		case ResourceTypeVolumeSize:
			leftQuotas.VolumeSize.Name = ResourceTypeVolumeSize
			leftQuotas.VolumeSize.Count = qcservice.IntValue(quotaLeftSet.Left)
		default:
			logger.Error(ctx, "Unknown quota type: %s", qcservice.StringValue(quotaLeftSet.ResourceType))
		}
	}

	err = needQuotas.LessThan(leftQuotas)
	if err != nil {
		logger.Error(ctx, "[%s] quota not enough: %+v", MyProvider, err)
		return err
	}

	return nil
}

func (p *ProviderHandler) DescribeVpc(ctx context.Context, runtimeId, vpcId string) (*models.Vpc, error) {
	qingcloudService, err := p.initService(ctx, runtimeId)
	if err != nil {
		logger.Error(ctx, "Init %s api service failed: %+v", MyProvider, err)
		return nil, err
	}

	routerService, err := qingcloudService.Router(qingcloudService.Config.Zone)
	if err != nil {
		logger.Error(ctx, "Init %s router api service failed: %+v", MyProvider, err)
		return nil, err
	}

	output, err := routerService.DescribeRouters(
		&qcservice.DescribeRoutersInput{
			Routers: qcservice.StringSlice([]string{vpcId}),
		},
	)
	if err != nil {
		logger.Error(ctx, "DescribeRouters to %s failed: %+v", MyProvider, err)
		return nil, err
	}

	retCode := qcservice.IntValue(output.RetCode)
	if retCode != 0 {
		message := qcservice.StringValue(output.Message)
		logger.Error(ctx, "Send DescribeRouters to %s failed with return code [%d], message [%s]",
			MyProvider, retCode, message)
		return nil, fmt.Errorf("send DescribeRouters to %s failed: %s", MyProvider, message)
	}

	if len(output.RouterSet) == 0 {
		logger.Error(ctx, "Send DescribeRouters to %s failed with 0 output instances", MyProvider)
		return nil, fmt.Errorf("send DescribeRouters to %s failed with 0 output instances", MyProvider)
	}

	vpc := output.RouterSet[0]

	var subnets []string
	for _, subnet := range vpc.VxNets {
		subnets = append(subnets, qcservice.StringValue(subnet.VxNetID))
	}

	return &models.Vpc{
		VpcId:            qcservice.StringValue(vpc.RouterID),
		Name:             qcservice.StringValue(vpc.RouterName),
		CreateTime:       qcservice.TimeValue(vpc.CreateTime),
		Description:      qcservice.StringValue(vpc.Description),
		Status:           qcservice.StringValue(vpc.Status),
		TransitionStatus: qcservice.StringValue(vpc.TransitionStatus),
		Subnets:          subnets,
		Eip: &models.Eip{
			EipId: qcservice.StringValue(vpc.EIP.EIPID),
			Name:  qcservice.StringValue(vpc.EIP.EIPName),
			Addr:  qcservice.StringValue(vpc.EIP.EIPAddr),
		},
	}, nil
}

func (p *ProviderHandler) DescribeZones(ctx context.Context, url, credential string) ([]string, error) {
	qingcloudService, err := p.initQingCloudService(ctx, url, credential, "")
	if err != nil {
		logger.Error(ctx, "Init %s api service failed: %+v", MyProvider, err)
		return nil, err
	}

	output, err := qingcloudService.DescribeZones(
		&qcservice.DescribeZonesInput{
			Status: qcservice.StringSlice([]string{"active"}),
		},
	)
	if err != nil {
		logger.Error(ctx, "DescribeZones to %s failed: %+v", MyProvider, err)
		return nil, err
	}

	retCode := qcservice.IntValue(output.RetCode)
	if retCode != 0 {
		message := qcservice.StringValue(output.Message)
		logger.Error(ctx, "Send DescribeZones to %s failed with return code [%d], message [%s]",
			MyProvider, retCode, message)
		return nil, fmt.Errorf("send DescribeZones to %s failed: %s", MyProvider, message)
	}

	var zones []string
	for _, zone := range output.ZoneSet {
		zones = append(zones, qcservice.StringValue(zone.ZoneID))
	}
	return zones, nil
}
