// The MIT License (MIT)

// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package nosql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
	"github.com/uber/cadence/common/persistence/serialization"
	"github.com/uber/cadence/common/types"
)

func testFixtureInternalShardInfo() *persistence.InternalShardInfo {
	now := time.Unix(100000, 0)
	return &persistence.InternalShardInfo{
		ShardID:             1,
		Owner:               "test-owner",
		RangeID:             101,
		StolenSinceRenew:    0,
		UpdatedAt:           now,
		ReplicationAckLevel: 100,
		ReplicationDLQAckLevel: map[string]int64{
			"cluster-1": 10,
			"cluster-2": 15,
		},
		TransferAckLevel: 50,
		TimerAckLevel:    now.Add(-time.Hour),
		ClusterTransferAckLevel: map[string]int64{
			"cluster-1": 60,
			"cluster-2": 70,
		},
		ClusterTimerAckLevel: map[string]time.Time{
			"cluster-1": now.Add(-time.Minute * 30),
			"cluster-2": now.Add(-time.Minute * 60),
		},
		TransferProcessingQueueStates: &persistence.DataBlob{
			Encoding: "base64",
			Data:     []byte("transfer-processing-states"),
		},
		TimerProcessingQueueStates: &persistence.DataBlob{
			Encoding: "base64",
			Data:     []byte("timer-processing-states"),
		},
		ClusterReplicationLevel: map[string]int64{
			"cluster-1": 200,
			"cluster-2": 250,
		},
		DomainNotificationVersion: 102,
		PendingFailoverMarkers: &persistence.DataBlob{
			Encoding: "base64",
			Data:     []byte("pending-failover-markers"),
		},
		QueueStates: map[int32]*types.QueueState{
			0: &types.QueueState{
				VirtualQueueStates: map[int64]*types.VirtualQueueState{
					0: {},
				},
			},
		},
	}
}

func setUpMocksForShardStore(t *testing.T) (*nosqlShardStore, *nosqlplugin.MockDB, *MockshardedNosqlStore, *serialization.MockParser, *gomock.Controller) {
	ctrl := gomock.NewController(t)
	dbMock := nosqlplugin.NewMockDB(ctrl)
	storeShardMock := NewMockshardedNosqlStore(ctrl)
	mockParser := serialization.NewMockParser(ctrl)

	shardStore := &nosqlShardStore{
		shardedNosqlStore:  storeShardMock,
		currentClusterName: "test-cluster",
		parser:             mockParser,
	}

	return shardStore, dbMock, storeShardMock, mockParser, ctrl
}

func TestCreateShard(t *testing.T) {
	testCases := []struct {
		name          string
		setupMock     func(*nosqlplugin.MockDB, *MockshardedNosqlStore, *serialization.MockParser)
		request       *persistence.InternalCreateShardRequest
		expectError   bool
		expectedError string
	}{
		{
			name: "success",
			setupMock: func(dbMock *nosqlplugin.MockDB, storeShardMock *MockshardedNosqlStore, mockParser *serialization.MockParser) {
				storeShardMock.EXPECT().GetStoreShardByHistoryShard(1).Return(&nosqlStore{db: dbMock}, nil).Times(1)
				mockParser.EXPECT().ShardInfoToBlob(gomock.Any()).Return(persistence.DataBlob{
					Encoding: "base64",
					Data:     []byte("shard-info"),
				}, nil).Times(1)
				dbMock.EXPECT().InsertShard(gomock.Any(), &nosqlplugin.ShardRow{
					InternalShardInfo: testFixtureInternalShardInfo(),
					Data:              []byte("shard-info"),
					DataEncoding:      "base64",
				}).Return(nil).Times(1)
			},
			request: &persistence.InternalCreateShardRequest{
				ShardInfo: testFixtureInternalShardInfo(),
			},
			expectError: false,
		},
		{
			name: "shard already exists error",
			setupMock: func(dbMock *nosqlplugin.MockDB, storeShardMock *MockshardedNosqlStore, mockParser *serialization.MockParser) {
				storeShardMock.EXPECT().GetStoreShardByHistoryShard(1).Return(&nosqlStore{db: dbMock}, nil).Times(1)
				mockParser.EXPECT().ShardInfoToBlob(gomock.Any()).Return(persistence.DataBlob{
					Encoding: "base64",
					Data:     []byte("shard-info"),
				}, nil).Times(1)
				dbMock.EXPECT().InsertShard(gomock.Any(), gomock.Any()).Return(&nosqlplugin.ShardOperationConditionFailure{
					RangeID: 200,
					Details: "rangeID mismatch",
				}).Times(1)
			},
			request: &persistence.InternalCreateShardRequest{
				ShardInfo: &persistence.InternalShardInfo{
					ShardID: 1,
					RangeID: 100,
				},
			},
			expectError:   true,
			expectedError: "Shard already exists in executions table.  ShardId: 1, request_range_id: 100, actual_range_id : 200, columns: (rangeID mismatch)",
		},
		{
			name: "generic db error",
			setupMock: func(dbMock *nosqlplugin.MockDB, storeShardMock *MockshardedNosqlStore, mockParser *serialization.MockParser) {
				storeShardMock.EXPECT().GetStoreShardByHistoryShard(1).Return(&nosqlStore{db: dbMock}, nil).Times(1)
				mockParser.EXPECT().ShardInfoToBlob(gomock.Any()).Return(persistence.DataBlob{
					Encoding: "base64",
					Data:     []byte("shard-info"),
				}, nil).Times(1)
				dbMock.EXPECT().InsertShard(gomock.Any(), gomock.Any()).Return(errors.New("db error")).Times(1)
				dbMock.EXPECT().IsNotFoundError(gomock.Any()).Return(true).Times(1)
			},
			request: &persistence.InternalCreateShardRequest{
				ShardInfo: &persistence.InternalShardInfo{
					ShardID: 1,
					RangeID: 100,
				},
			},
			expectError:   true,
			expectedError: "CreateShard failed. Error: db error ",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup mocks for each test case individually
			shardStore, dbMock, storeShardMock, mockParser, ctrl := setUpMocksForShardStore(t)
			defer ctrl.Finish()

			// Setup test-specific mock behavior
			tc.setupMock(dbMock, storeShardMock, mockParser)

			// Execute the method under test
			err := shardStore.CreateShard(context.Background(), tc.request)

			// Validate results
			if tc.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetShard(t *testing.T) {
	testCases := []struct {
		name          string
		setupMock     func(*nosqlplugin.MockDB, *MockshardedNosqlStore, *serialization.MockParser)
		request       *persistence.InternalGetShardRequest
		expectError   bool
		expected      *persistence.InternalGetShardResponse
		expectedError string
	}{
		{
			name: "success - no update",
			setupMock: func(dbMock *nosqlplugin.MockDB, storeShardMock *MockshardedNosqlStore, mockParser *serialization.MockParser) {
				storeShardMock.EXPECT().GetStoreShardByHistoryShard(1).Return(&nosqlStore{db: dbMock, dc: &persistence.DynamicConfiguration{
					ReadNoSQLShardFromDataBlob: dynamicproperties.GetBoolPropertyFn(true),
				}}, nil).Times(1)
				shardInfo := testFixtureInternalShardInfo()
				dbMock.EXPECT().SelectShard(gomock.Any(), 1, "test-cluster").Return(int64(101), &nosqlplugin.ShardRow{
					InternalShardInfo: shardInfo,
					Data:              []byte("shard-info"),
					DataEncoding:      "base64",
				}, nil).Times(1)
				storeShardMock.EXPECT().GetMetricsClient().Return(metrics.NewNoopMetricsClient()).Times(1)
				mockParser.EXPECT().ShardInfoFromBlob(gomock.Any(), gomock.Any()).Return(&serialization.ShardInfo{
					Owner:               "test-owner",
					StolenSinceRenew:    1,
					UpdatedAt:           time.Unix(100000, 0),
					ReplicationAckLevel: 100,
					TransferAckLevel:    50,
					TimerAckLevel:       time.Unix(100000, 0),
					ClusterTransferAckLevel: map[string]int64{
						"cluster-1": 60,
						"cluster-2": 70,
					},
					ClusterTimerAckLevel: map[string]time.Time{
						"cluster-1": time.Unix(100000, 0),
						"cluster-2": time.Unix(100000, 0),
					},
					TransferProcessingQueueStates:         []byte("transfer-processing-states"),
					TransferProcessingQueueStatesEncoding: "base64",
					TimerProcessingQueueStates:            []byte("timer-processing-states"),
					TimerProcessingQueueStatesEncoding:    "base64",
					ClusterReplicationLevel: map[string]int64{
						"cluster-1": 200,
						"cluster-2": 250,
					},
					DomainNotificationVersion:      102,
					PendingFailoverMarkers:         []byte("pending-failover-markers"),
					PendingFailoverMarkersEncoding: "base64",
					ReplicationDlqAckLevel: map[string]int64{
						"cluster-1": 10,
						"cluster-2": 15,
					},
					QueueStates: map[int32]*types.QueueState{
						0: &types.QueueState{
							VirtualQueueStates: map[int64]*types.VirtualQueueState{
								0: {},
							},
						},
					},
				}, nil).Times(1)
			},
			request:     &persistence.InternalGetShardRequest{ShardID: 1},
			expectError: false,
			expected: &persistence.InternalGetShardResponse{
				ShardInfo: &persistence.InternalShardInfo{
					ShardID:             1,
					RangeID:             101,
					Owner:               "test-owner",
					StolenSinceRenew:    1,
					UpdatedAt:           time.Unix(100000, 0),
					ReplicationAckLevel: 100,
					TransferAckLevel:    50,
					TimerAckLevel:       time.Unix(100000, 0),
					ClusterTransferAckLevel: map[string]int64{
						"cluster-1": 60,
						"cluster-2": 70,
					},
					ClusterTimerAckLevel: map[string]time.Time{
						"cluster-1": time.Unix(100000, 0),
						"cluster-2": time.Unix(100000, 0),
					},
					TransferProcessingQueueStates: &persistence.DataBlob{
						Encoding: "base64",
						Data:     []byte("transfer-processing-states"),
					},
					TimerProcessingQueueStates: &persistence.DataBlob{
						Encoding: "base64",
						Data:     []byte("timer-processing-states"),
					},
					ClusterReplicationLevel: map[string]int64{
						"cluster-1": 200,
						"cluster-2": 250,
					},
					DomainNotificationVersion: 102,
					PendingFailoverMarkers: &persistence.DataBlob{
						Encoding: "base64",
						Data:     []byte("pending-failover-markers"),
					},
					ReplicationDLQAckLevel: map[string]int64{
						"cluster-1": 10,
						"cluster-2": 15,
					},
					QueueStates: map[int32]*types.QueueState{
						0: &types.QueueState{
							VirtualQueueStates: map[int64]*types.VirtualQueueState{
								0: {},
							},
						},
					},
				},
			},
		},
		{
			name: "success - fix shard",
			setupMock: func(dbMock *nosqlplugin.MockDB, storeShardMock *MockshardedNosqlStore, mockParser *serialization.MockParser) {
				storeShardMock.EXPECT().GetStoreShardByHistoryShard(1).Return(&nosqlStore{db: dbMock, dc: &persistence.DynamicConfiguration{
					ReadNoSQLShardFromDataBlob: dynamicproperties.GetBoolPropertyFn(false),
				}}, nil).Times(2)
				storeShardMock.EXPECT().GetLogger().Return(log.NewNoop()).Times(1)
				storeShardMock.EXPECT().GetMetricsClient().Return(metrics.NewNoopMetricsClient()).Times(1)
				dbMock.EXPECT().SelectShard(gomock.Any(), 1, "test-cluster").Return(int64(100), &nosqlplugin.ShardRow{
					InternalShardInfo: testFixtureInternalShardInfo(),
					Data:              []byte("shard-info"),
					DataEncoding:      "base64",
				}, nil).Times(1)
				dbMock.EXPECT().UpdateRangeID(gomock.Any(), 1, int64(101), int64(100)).Return(nil).Times(1)
			},
			request:     &persistence.InternalGetShardRequest{ShardID: 1},
			expectError: false,
			expected: &persistence.InternalGetShardResponse{
				ShardInfo: testFixtureInternalShardInfo(),
			},
		},
		{
			name: "error fixing shard - shard ownership lost error",
			setupMock: func(dbMock *nosqlplugin.MockDB, storeShardMock *MockshardedNosqlStore, mockParser *serialization.MockParser) {
				storeShardMock.EXPECT().GetStoreShardByHistoryShard(1).Return(&nosqlStore{db: dbMock, dc: &persistence.DynamicConfiguration{
					ReadNoSQLShardFromDataBlob: dynamicproperties.GetBoolPropertyFn(false),
				}}, nil).Times(2)
				storeShardMock.EXPECT().GetLogger().Return(log.NewNoop()).Times(1)
				dbMock.EXPECT().SelectShard(gomock.Any(), 1, "test-cluster").Return(int64(100), &nosqlplugin.ShardRow{
					InternalShardInfo: testFixtureInternalShardInfo(),
					Data:              []byte("shard-info"),
					DataEncoding:      "base64",
				}, nil).Times(1)
				dbMock.EXPECT().UpdateRangeID(gomock.Any(), 1, int64(101), int64(100)).Return(&nosqlplugin.ShardOperationConditionFailure{
					RangeID: 200,
					Details: "rangeID mismatch",
				}).Times(1)
			},
			request:       &persistence.InternalGetShardRequest{ShardID: 1},
			expectError:   true,
			expectedError: "Failed to update shard rangeID.  request_range_id: 100, actual_range_id : 200, columns: (rangeID mismatch)",
		},
		{
			name: "error fixing shard - generic db error",
			setupMock: func(dbMock *nosqlplugin.MockDB, storeShardMock *MockshardedNosqlStore, mockParser *serialization.MockParser) {
				storeShardMock.EXPECT().GetStoreShardByHistoryShard(1).Return(&nosqlStore{db: dbMock, dc: &persistence.DynamicConfiguration{
					ReadNoSQLShardFromDataBlob: dynamicproperties.GetBoolPropertyFn(false),
				}}, nil).Times(2)
				storeShardMock.EXPECT().GetLogger().Return(log.NewNoop()).Times(1)
				dbMock.EXPECT().SelectShard(gomock.Any(), 1, "test-cluster").Return(int64(100), &nosqlplugin.ShardRow{
					InternalShardInfo: testFixtureInternalShardInfo(),
					Data:              []byte("shard-info"),
					DataEncoding:      "base64",
				}, nil).Times(1)
				dbMock.EXPECT().UpdateRangeID(gomock.Any(), 1, int64(101), int64(100)).Return(errors.New("db error")).Times(1)
				dbMock.EXPECT().IsNotFoundError(gomock.Any()).Return(true).Times(1)
			},
			request:       &persistence.InternalGetShardRequest{ShardID: 1},
			expectError:   true,
			expectedError: "UpdateRangeID failed. Error: db error ",
		},
		{
			name: "shard not found error",
			setupMock: func(dbMock *nosqlplugin.MockDB, storeShardMock *MockshardedNosqlStore, mockParser *serialization.MockParser) {
				storeShardMock.EXPECT().GetStoreShardByHistoryShard(1).Return(&nosqlStore{db: dbMock}, nil).Times(1)
				dbMock.EXPECT().SelectShard(gomock.Any(), 1, "test-cluster").Return(int64(0), nil, errors.New("not found")).Times(1)
				dbMock.EXPECT().IsNotFoundError(errors.New("not found")).Return(true).Times(1)
			},
			request:       &persistence.InternalGetShardRequest{ShardID: 1},
			expectError:   true,
			expectedError: "Shard not found.  ShardId: 1",
		},
		{
			name: "generic db error",
			setupMock: func(dbMock *nosqlplugin.MockDB, storeShardMock *MockshardedNosqlStore, mockParser *serialization.MockParser) {
				storeShardMock.EXPECT().GetStoreShardByHistoryShard(1).Return(&nosqlStore{db: dbMock}, nil).Times(1)
				dbMock.EXPECT().SelectShard(gomock.Any(), 1, "test-cluster").Return(int64(0), nil, errors.New("db error")).Times(1)
				dbMock.EXPECT().IsNotFoundError(gomock.Any()).Return(false).Times(1)
				dbMock.EXPECT().IsNotFoundError(gomock.Any()).Return(true).Times(1)
			},
			request:       &persistence.InternalGetShardRequest{ShardID: 1},
			expectError:   true,
			expectedError: "GetShard failed. Error: db error ",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup mocks for each test case individually
			shardStore, dbMock, storeShardMock, mockParser, ctrl := setUpMocksForShardStore(t)
			defer ctrl.Finish()

			// Setup test-specific mock behavior
			tc.setupMock(dbMock, storeShardMock, mockParser)

			// Execute the method under test
			resp, err := shardStore.GetShard(context.Background(), tc.request)

			// Validate results
			if tc.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedError)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, resp)
			}
		})
	}
}

func TestUpdateShard(t *testing.T) {
	testCases := []struct {
		name          string
		setupMock     func(*nosqlplugin.MockDB, *MockshardedNosqlStore, *serialization.MockParser)
		request       *persistence.InternalUpdateShardRequest
		expectError   bool
		expectedError string
	}{
		{
			name: "success",
			setupMock: func(dbMock *nosqlplugin.MockDB, storeShardMock *MockshardedNosqlStore, mockParser *serialization.MockParser) {
				storeShardMock.EXPECT().GetStoreShardByHistoryShard(1).Return(&nosqlStore{db: dbMock}, nil).Times(1)
				mockParser.EXPECT().ShardInfoToBlob(gomock.Any()).Return(persistence.DataBlob{
					Encoding: "base64",
					Data:     []byte("shard-info"),
				}, nil).Times(1)
				dbMock.EXPECT().UpdateShard(gomock.Any(), &nosqlplugin.ShardRow{
					InternalShardInfo: testFixtureInternalShardInfo(),
					Data:              []byte("shard-info"),
					DataEncoding:      "base64",
				}, int64(100)).Return(nil).Times(1)
			},
			request: &persistence.InternalUpdateShardRequest{
				ShardInfo:       testFixtureInternalShardInfo(),
				PreviousRangeID: 100,
			},
			expectError: false,
		},
		{
			name: "shard ownership lost error",
			setupMock: func(dbMock *nosqlplugin.MockDB, storeShardMock *MockshardedNosqlStore, mockParser *serialization.MockParser) {
				storeShardMock.EXPECT().GetStoreShardByHistoryShard(1).Return(&nosqlStore{db: dbMock}, nil).Times(1)
				mockParser.EXPECT().ShardInfoToBlob(gomock.Any()).Return(persistence.DataBlob{
					Encoding: "base64",
					Data:     []byte("shard-info"),
				}, nil).Times(1)
				dbMock.EXPECT().UpdateShard(gomock.Any(), gomock.Any(), int64(100)).Return(&nosqlplugin.ShardOperationConditionFailure{
					RangeID: 200,
					Details: "rangeID mismatch",
				}).Times(1)
			},
			request: &persistence.InternalUpdateShardRequest{
				ShardInfo: &persistence.InternalShardInfo{
					ShardID: 1,
					RangeID: 100,
				},
				PreviousRangeID: 100,
			},
			expectError:   true,
			expectedError: "Failed to update shard rangeID.  request_range_id: 100, actual_range_id : 200, columns: (rangeID mismatch)",
		},
		{
			name: "generic db error",
			setupMock: func(dbMock *nosqlplugin.MockDB, storeShardMock *MockshardedNosqlStore, mockParser *serialization.MockParser) {
				storeShardMock.EXPECT().GetStoreShardByHistoryShard(1).Return(&nosqlStore{db: dbMock}, nil).Times(1)
				dbMock.EXPECT().UpdateShard(gomock.Any(), gomock.Any(), int64(100)).Return(errors.New("db error")).Times(1)
				mockParser.EXPECT().ShardInfoToBlob(gomock.Any()).Return(persistence.DataBlob{
					Encoding: "base64",
					Data:     []byte("shard-info"),
				}, nil).Times(1)
				dbMock.EXPECT().IsNotFoundError(gomock.Any()).Return(true).Times(1)
			},
			request: &persistence.InternalUpdateShardRequest{
				ShardInfo: &persistence.InternalShardInfo{
					ShardID: 1,
					RangeID: 100,
				},
				PreviousRangeID: 100,
			},
			expectError:   true,
			expectedError: "UpdateShard failed. Error: db error ",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup mocks for each test case individually
			shardStore, dbMock, storeShardMock, mockParser, ctrl := setUpMocksForShardStore(t)
			defer ctrl.Finish()

			// Setup test-specific mock behavior
			tc.setupMock(dbMock, storeShardMock, mockParser)

			// Execute the method under test
			err := shardStore.UpdateShard(context.Background(), tc.request)

			// Validate results
			if tc.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestUpdateRangeID(t *testing.T) {
	testCases := []struct {
		name            string
		setupMock       func(*nosqlplugin.MockDB, *MockshardedNosqlStore, *serialization.MockParser)
		shardID         int
		rangeID         int64
		previousRangeID int64
		expectError     bool
		expectedError   string
	}{
		{
			name: "success",
			setupMock: func(dbMock *nosqlplugin.MockDB, storeShardMock *MockshardedNosqlStore, mockParser *serialization.MockParser) {
				storeShardMock.EXPECT().GetStoreShardByHistoryShard(1).Return(&nosqlStore{db: dbMock}, nil).Times(1)
				dbMock.EXPECT().UpdateRangeID(gomock.Any(), 1, int64(100), int64(99)).Return(nil).Times(1)
			},
			shardID:         1,
			rangeID:         100,
			previousRangeID: 99,
			expectError:     false,
		},
		{
			name: "shard ownership lost error",
			setupMock: func(dbMock *nosqlplugin.MockDB, storeShardMock *MockshardedNosqlStore, mockParser *serialization.MockParser) {
				storeShardMock.EXPECT().GetStoreShardByHistoryShard(1).Return(&nosqlStore{db: dbMock}, nil).Times(1)
				dbMock.EXPECT().UpdateRangeID(gomock.Any(), 1, int64(100), int64(99)).Return(&nosqlplugin.ShardOperationConditionFailure{
					RangeID: 200,
					Details: "rangeID mismatch",
				}).Times(1)
			},
			shardID:         1,
			rangeID:         100,
			previousRangeID: 99,
			expectError:     true,
			expectedError:   "Failed to update shard rangeID.  request_range_id: 99, actual_range_id : 200, columns: (rangeID mismatch)",
		},
		{
			name: "generic db error",
			setupMock: func(dbMock *nosqlplugin.MockDB, storeShardMock *MockshardedNosqlStore, mockParser *serialization.MockParser) {
				storeShardMock.EXPECT().GetStoreShardByHistoryShard(1).Return(&nosqlStore{db: dbMock}, nil).Times(1)
				dbMock.EXPECT().UpdateRangeID(gomock.Any(), 1, int64(100), int64(99)).Return(errors.New("db error")).Times(1)
				dbMock.EXPECT().IsNotFoundError(gomock.Any()).Return(true).Times(1)
			},
			shardID:         1,
			rangeID:         100,
			previousRangeID: 99,
			expectError:     true,
			expectedError:   "UpdateRangeID failed. Error: db error ",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup mocks for each test case individually
			shardStore, dbMock, storeShardMock, mockParser, ctrl := setUpMocksForShardStore(t)
			defer ctrl.Finish()

			// Setup test-specific mock behavior
			tc.setupMock(dbMock, storeShardMock, mockParser)

			// Execute the method under test
			err := shardStore.updateRangeID(context.Background(), tc.shardID, tc.rangeID, tc.previousRangeID)

			// Validate results
			if tc.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
