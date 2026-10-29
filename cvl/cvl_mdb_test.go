////////////////////////////////////////////////////////////////////////////////
//                                                                            //
//  Copyright 2026 Broadcom. The term Broadcom refers to Broadcom Inc. and/or //
//  its subsidiaries.                                                         //
//                                                                            //
//  Licensed under the Apache License, Version 2.0 (the "License");           //
//  you may not use this file except in compliance with the License.          //
//  You may obtain a copy of the License at                                   //
//                                                                            //
//     http://www.apache.org/licenses/LICENSE-2.0                             //
//                                                                            //
//  Unless required by applicable law or agreed to in writing, software       //
//  distributed under the License is distributed on an "AS IS" BASIS,         //
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  //
//  See the License for the specific language governing permissions and       //
//  limitations under the License.                                            //
//                                                                            //
////////////////////////////////////////////////////////////////////////////////

package cvl_test

import (
	"testing"

	cmn "github.com/Azure/sonic-mgmt-common/cvl/common"
	"github.com/go-redis/redis/v7"

	. "github.com/Azure/sonic-mgmt-common/cvl/internal/util"
)

func skipUnlessMultiAsic(t *testing.T) {
	t.Helper()
	if NumAsic <= 1 || len(MdbNames) < 2 {
		t.Skip("multi-ASIC CVL tests require NumAsic > 1 and per-namespace DB config")
	}
}

func configRedisForNamespace(ns string) *redis.Client {
	if ns == hostDBName {
		return NewDbClient("CONFIG_DB")
	}
	return NewDbClient("CONFIG_DB." + ns)
}

func setupTestDataForNamespace(t *testing.T, ns string, mpi map[string]interface{}) {
	t.Helper()
	client := configRedisForNamespace(ns)
	loadConfigDB(client, mpi)
	t.Cleanup(func() { unloadConfigDB(client, mpi) })
}

// VXLAN_TUNNEL allows at most one entry. Data in asic0 must not count toward asic1.
func TestValidateEditConfig_VxlanMaxElements_IsolatedPerNamespace(t *testing.T) {
	skipUnlessMultiAsic(t)

	const asic0 = "asic0"
	const asic1 = "asic1"

	setupTestDataForNamespace(t, asic0, map[string]interface{}{
		"VXLAN_TUNNEL": map[string]interface{}{
			"tun-asic0": map[string]interface{}{
				"src_ip": "10.0.0.1",
			},
		},
	})

	t.Run("asic0_second_tunnel_rejected", func(tt *testing.T) {
		cvSess := NewTestSessionForDB(tt, getMDBInstance(asic0))
		cvlErrInfo, _ := cvSess.ValidateEditConfig([]cmn.CVLEditConfigData{{
			cmn.VALIDATE_ALL,
			cmn.OP_CREATE,
			"VXLAN_TUNNEL|tun-asic0-b",
			map[string]string{"src_ip": "10.0.0.2"},
			false,
		}})
		verifyErr(tt, cvlErrInfo, CVLErrorInfo{
			ErrCode:          CVL_SYNTAX_ERROR,
			TableName:        "VXLAN_TUNNEL",
			Keys:             []string{"tun-asic0-b"},
			Msg:              "Max elements limit reached",
			ConstraintErrMsg: "Max elements limit 1 reached",
			ErrAppTag:        "too-many-elements",
		})
	})

	t.Run("asic1_first_tunnel_allowed", func(tt *testing.T) {
		cvSess := NewTestSessionForDB(tt, getMDBInstance(asic1))
		cvlErrInfo, _ := cvSess.ValidateEditConfig([]cmn.CVLEditConfigData{{
			cmn.VALIDATE_ALL,
			cmn.OP_CREATE,
			"VXLAN_TUNNEL|tun-asic1",
			map[string]string{"src_ip": "20.0.0.1"},
			false,
		}})
		verifyErr(tt, cvlErrInfo, Success)
	})
}

// Host namespace session must not see CONFIG_DB rows stored only under asic0.
func TestValidateEditConfig_HostSessionIgnoresAsicOnlyData(t *testing.T) {
	skipUnlessMultiAsic(t)

	setupTestDataForNamespace(t, "asic0", map[string]interface{}{
		"VLAN": map[string]interface{}{
			"Vlan2001": map[string]interface{}{
				"vlanid": "2001",
			},
		},
	})

	cvSess := NewTestSession(t)
	cvlErrInfo, _ := cvSess.ValidateEditConfig([]cmn.CVLEditConfigData{{
		cmn.VALIDATE_ALL,
		cmn.OP_CREATE,
		"VLAN|Vlan2001",
		map[string]string{"vlanid": "2001"},
		false,
	}})
	verifyErr(t, cvlErrInfo, Success)
}
