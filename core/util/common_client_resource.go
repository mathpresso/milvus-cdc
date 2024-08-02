package util

import (
	"fmt"
	"github.com/milvus-io/milvus/pkg/util/resource"
	"sync"
)

var (
	clientManager     *ClientResourceManager
	clientManagerOnce sync.Once
)

type ClientResourceManager struct {
	manager resource.Manager
}

func GetMilvusClientManager() *ClientResourceManager {
	clientManagerOnce.Do(func() {
		manager := resource.NewManager(0, 0, nil)
		clientManager = &ClientResourceManager{
			manager: manager,
		}
	})
	return clientManager
}

func GetAPIKey(username, password string) string {
	return fmt.Sprintf("%s:%s", username, password)
}

func GetBigQueryClientManager() *ClientResourceManager {
	clientManagerOnce.Do(func() {
		manager := resource.NewManager(0, 0, nil)
		clientManager = &ClientResourceManager{
			manager: manager,
		}
	})
	return clientManager
}

func GetMySqlClientManager() *ClientResourceManager {
	clientManagerOnce.Do(func() {
		manager := resource.NewManager(0, 0, nil)
		clientManager = &ClientResourceManager{
			manager: manager,
		}
	})
	return clientManager
}
