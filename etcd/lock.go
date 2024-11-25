package etcd

import (
	"context"
	"fmt"

	"github.com/DeltaNicola/infralib/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func AcquireLock(client *clientv3.Client, lockKey string, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	logger.Logger.Info(
		"Acquiring Lock",
		zap.String("lockKey", lockKey),
		zap.Int64("ttl", ttl),
	)

	resp, err := client.Get(context.Background(), lockKey)
	if err != nil {
		logger.Logger.Error(
			"Error Checking Lock Key",
			zap.String("lockKey", lockKey),
			zap.Error(err),
		)
		return nil, fmt.Errorf("errore durante il controllo della chiave %s: %v", lockKey, err)
	}

	if len(resp.Kvs) > 0 {
		logger.Logger.Warn(
			"Lock Key Already Exists",
			zap.String("lockKey", lockKey),
		)
		return nil, fmt.Errorf("la chiave %s è già occupata", lockKey)
	}

	leaseResp, err := client.Grant(context.Background(), ttl)
	if err != nil {
		logger.Logger.Error(
			"Failed Creating Lease",
			zap.String("lockKey", lockKey),
			zap.Error(err),
		)
		return nil, fmt.Errorf("errore durante la creazione del lease: %v", err)
	}

	txn := client.Txn(context.Background()).
		If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseResp.ID)))

	txnResp, err := txn.Commit()
	if err != nil {
		logger.Logger.Error(
			"Transaction Error",
			zap.String("lockKey", lockKey),
			zap.Error(err),
		)
		return nil, fmt.Errorf("errore durante la transazione per il lock: %v", err)
	}

	if !txnResp.Succeeded {
		logger.Logger.Warn(
			"Lock Acquisition Failed",
			zap.String("reason", "key already occupied"),
			zap.String("lockKey", lockKey),
		)
		return nil, fmt.Errorf("il lock non è stato acquisito, chiave già occupata")
	}

	logger.Logger.Info(
		"Lock acquired successfully",
		zap.String("lockKey", lockKey),
		zap.Reflect("leaseID", leaseResp.ID),
	)

	return leaseResp, nil
}

func ReleaseOrderLock(client *clientv3.Client, lockKey string, leaseID clientv3.LeaseID) error {
	logger.Logger.Info(
		"Releasing Lock",
		zap.String("lockKey", lockKey),
		zap.Reflect("leaseID", leaseID),
	)

	resp, err := client.Get(context.Background(), lockKey)
	if err != nil {
		logger.Logger.Error(
			"Error Checking Lock Key",
			zap.String("lockKey", lockKey),
			zap.Error(err),
		)
		return fmt.Errorf("errore durante il controllo della chiave %s: %v", lockKey, err)
	}

	if len(resp.Kvs) == 0 {
		logger.Logger.Warn(
			"Lock Key Already Deleted",
			zap.String("lockKey", lockKey),
		)
		return fmt.Errorf("la chiave %s non esiste già più", lockKey)
	}

	_, err = client.Delete(context.Background(), lockKey)
	if err != nil {
		logger.Logger.Error(
			"Error Deleting Lock Key",
			zap.String("lockKey", lockKey),
			zap.Error(err),
		)
		return fmt.Errorf("errore durante la rimozione del lock: %v", err)
	}

	logger.Logger.Info(
		"Lock Key Deleted Successfully",
		zap.String("lockKey", lockKey),
	)

	_, err = client.Revoke(context.Background(), leaseID)
	if err != nil {
		logger.Logger.Error(
			"Error Revoking Lease",
			zap.Reflect("leaseID", leaseID),
			zap.Error(err),
		)
		return fmt.Errorf("errore durante la revoca del lease: %v", err)
	}

	logger.Logger.Info(
		"Lease Revoked Successfully",
		zap.Reflect("leaseID", leaseID),
	)
	return nil
}

func WaitForLockRelease(ctx context.Context, client *clientv3.Client, lockKey string, interfaceAction interface{}, releaseAction func(interface{})) {
	logger.Logger.Info(
		"Waiting for lock release",
		zap.String("lockKey", lockKey),
	)

	rch := client.Watch(ctx, lockKey)

	for {
		select {
		case <-ctx.Done():
			logger.Logger.Warn(
				"Context canceled while waiting for lock release",
				zap.String("lockKey", lockKey),
			)
			return
		case wresp := <-rch:
			for _, ev := range wresp.Events {
				if ev.Type == clientv3.EventTypeDelete {
					logger.Logger.Info(
						"Lock released, executing release action",
						zap.String("lockKey", lockKey),
					)
					releaseAction(interfaceAction)
					return
				}
			}
		}
	}
}
