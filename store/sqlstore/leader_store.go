package sqlstore

import (
	"github.com/mattermost/focalboard/server/utils"
	"github.com/mattermost/mattermost-server/v6/einterfaces"
	"github.com/mattermost/mattermost-server/v6/model"
	"github.com/mattermost/mattermost-server/v6/store"
	"github.com/pkg/errors"
)

type SqlLeaderStore struct {
	*SqlStore
	metrics einterfaces.MetricsInterface
}

func newSqlLeaderStore(sqlStore *SqlStore, metrics einterfaces.MetricsInterface) store.LeaderStore {
	return &SqlLeaderStore{
		SqlStore: sqlStore,
		metrics:  metrics,
	}
}

func (ls SqlLeaderStore) Save(leader *model.Leader) (*model.Leader, error) {
	if err := leader.IsValid(); err != nil {
		return nil, err
	}

	transaction, err := ls.GetMasterX().Beginx()
	if err != nil {
		return nil, errors.Wrap(err, "begin_transaction")
	}
	defer finalizeTransactionX(transaction, &err)

	if _, err := transaction.Exec(`LOCK Leader IN ACCESS EXCLUSIVE MODE NOWAIT`); err != nil {
		return nil, errors.Wrap(err, "error acquiring lock")
	}

	if _, err := transaction.Exec(`DELETE FROM Leader
		WHERE expireat < ?`, utils.GetMillis()); err != nil {
		return nil, errors.Wrap(err, "error deleting expired leader")
	}

	if _, err := transaction.NamedExec(`INSERT INTO Leader
		(Address, ExpireAt)
		VALUES
		(:Address, :ExpireAt)
		WHERE NOT EXISTS (SELECT * FROM Leader)`, leader); err != nil {
		return nil, errors.Wrap(err, "error saving leader")
	}

	if err = transaction.Commit(); err != nil {
		return nil, errors.Wrap(err, "commit_transaction")
	}

	return leader, nil
}

func (ls SqlLeaderStore) GetList() ([]*model.Leader, error) {
	leaders := []*model.Leader{}

	query := "SELECT * FROM Leader"

	if err := ls.GetReplicaX().Select(&leaders, query); err != nil {
		return nil, errors.Wrap(err, "could not get list of leaders")
	}
	return leaders, nil
}

func (ls SqlLeaderStore) Delete(leader *model.Leader) error {
	transaction, err := ls.GetMasterX().Beginx()
	if err != nil {
		return errors.Wrap(err, "begin_transaction")
	}
	defer finalizeTransactionX(transaction, &err)

	if _, err := transaction.Exec(`LOCK Leader IN ACCESS EXCLUSIVE MODE NOWAIT`); err != nil {
		return errors.Wrap(err, "error acquiring lock")
	}

	if sqlResult, err := transaction.Exec(
		`DELETE FROM
			Leader
		WHERE
			Address = ?`, leader.Address); err != nil {
		return errors.Wrap(err, "could not delete leader")
	} else if rows, err := sqlResult.RowsAffected(); rows == 0 {
		return store.NewErrNotFound("Leader", leader.Address).Wrap(err)
	}

	if err = transaction.Commit(); err != nil {
		return errors.Wrap(err, "commit_transaction")
	}

	return nil
}
