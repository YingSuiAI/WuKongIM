package meta

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/db/internal/dberrors"
	"github.com/WuKongIM/WuKongIM/pkg/db/internal/schema"
)

// Device stores per-device token state for a UID.
type Device struct {
	UID                    string
	DeviceFlag             int64
	DeviceID               string
	AppInstanceID          string
	DeviceSessionID        string
	IMSessionID            string
	InstallationGeneration uint64
	SessionGeneration      uint64
	AuthorizationFence     uint64
	Token                  string
	DeviceLevel            int64
}

const (
	deviceColumnUID uint16 = iota + 1
	deviceColumnFlag
	deviceColumnID
	deviceColumnAppInstanceID
	deviceColumnValue
)

var deviceTable = registerMetaTable(TableSpec[Device]{
	ID:   TableIDDevice,
	Name: "device",
	Columns: []schema.Column{
		{ID: deviceColumnUID, Name: "uid", Type: schema.TypeString, Required: true},
		{ID: deviceColumnFlag, Name: "device_flag", Type: schema.TypeInt64, Required: true},
		{ID: deviceColumnID, Name: "device_id", Type: schema.TypeString, Required: true},
		{ID: deviceColumnAppInstanceID, Name: "app_instance_id", Type: schema.TypeString, Required: true},
		{ID: deviceColumnValue, Name: "value", Type: schema.TypeBytes},
	},
	Families: []schema.Family{{ID: devicePrimaryFamilyID, Name: "primary", Columns: []uint16{deviceColumnValue}}},
	Primary: PrimarySpec[Device]{
		IndexID:  devicePrimaryIndexID,
		FamilyID: devicePrimaryFamilyID,
		Name:     "pk_device",
		Columns:  []uint16{deviceColumnUID, deviceColumnFlag, deviceColumnID, deviceColumnAppInstanceID},
		Layout:   KeyLayout{KeyString, KeyInt64Ordered, KeyString, KeyString},
		Key: func(device Device) KeyParts {
			return KeyParts{String(device.UID), Int64Ordered(device.DeviceFlag), String(device.DeviceID), String(device.AppInstanceID)}
		},
	},
	Validate: validateDevice,
	EncodeValue: func(device Device) ([]byte, error) {
		return encodeDeviceValue(device), nil
	},
	DecodeValue: func(primary KeyParts, value []byte) (Device, error) {
		return decodeDeviceValue(primary[0].S, primary[1].I64, primary[2].S, primary[3].S, value)
	},
})

// DeviceTable describes the device table schema.
var DeviceTable = deviceTable.Schema()

// UpsertDevice stores a device regardless of prior existence.
func (s *Shard) UpsertDevice(ctx context.Context, device Device) error {
	return deviceTable.Upsert(ctx, s, device)
}

// GetDevice returns one device by UID and device flag.
func (s *Shard) GetDevice(ctx context.Context, uid string, deviceFlag int64, deviceID, appInstanceID string) (Device, bool, error) {
	if err := validateKeyString(uid); err != nil {
		return Device{}, false, err
	}
	return deviceTable.Get(ctx, s, KeyParts{String(uid), Int64Ordered(deviceFlag), String(deviceID), String(appInstanceID)})
}

// ListDevicesByUID returns every installation token row for one user.
func (s *Shard) ListDevicesByUID(ctx context.Context, uid string) ([]Device, error) {
	if err := validateKeyString(uid); err != nil {
		return nil, err
	}
	rows, _, _, err := deviceTable.ScanPrimaryPrefix(ctx, s, KeyParts{String(uid)}, nil, 0)
	return rows, err
}

func validateDevice(device Device) error {
	if err := validateKeyString(device.UID); err != nil {
		return err
	}
	if err := validateKeyString(device.DeviceID); err != nil {
		return err
	}
	return validateKeyString(device.AppInstanceID)
}

func encodeDeviceValue(device Device) []byte {
	value := appendValueString(nil, device.Token)
	value = appendValueInt64(value, device.DeviceLevel)
	value = appendValueString(value, device.DeviceSessionID)
	value = appendValueString(value, device.IMSessionID)
	value = appendValueInt64(value, int64(device.InstallationGeneration))
	value = appendValueInt64(value, int64(device.SessionGeneration))
	value = appendValueInt64(value, int64(device.AuthorizationFence))
	return value
}

func decodeDeviceValue(uid string, deviceFlag int64, deviceID, appInstanceID string, value []byte) (Device, error) {
	token, rest, err := readValueString(value)
	if err != nil {
		return Device{}, err
	}
	deviceLevel, rest, err := readValueInt64(rest)
	if err != nil {
		return Device{}, err
	}
	deviceSessionID, rest, err := readValueString(rest)
	if err != nil {
		return Device{}, err
	}
	imSessionID, rest, err := readValueString(rest)
	if err != nil {
		return Device{}, err
	}
	installationGeneration, rest, err := readValueInt64(rest)
	if err != nil {
		return Device{}, err
	}
	sessionGeneration, rest, err := readValueInt64(rest)
	if err != nil {
		return Device{}, err
	}
	authorizationFence, rest, err := readValueInt64(rest)
	if err != nil {
		return Device{}, err
	}
	if len(rest) != 0 {
		return Device{}, dberrors.ErrCorruptValue
	}
	return Device{UID: uid, DeviceFlag: deviceFlag, DeviceID: deviceID, AppInstanceID: appInstanceID, DeviceSessionID: deviceSessionID, IMSessionID: imSessionID, InstallationGeneration: uint64(installationGeneration), SessionGeneration: uint64(sessionGeneration), AuthorizationFence: uint64(authorizationFence), Token: token, DeviceLevel: deviceLevel}, nil
}
