package wal

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/spf13/viper"
)

type Replicator struct {
	conn             *pgconn.PgConn
	relationStore    map[uint32]*pglogrepl.RelationMessage
	lastProcessedLSN pglogrepl.LSN // To track LSN for StandbyStatusUpdate
	slotName         string
	publicationName  string
}

func NewReplicator() (*Replicator, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s replication=database",
		viper.GetString("postgres.host"),
		viper.GetInt("postgres.port"),
		viper.GetString("postgres.user"),
		viper.GetString("postgres.password"),
		viper.GetString("postgres.dbname"),
	)

	conn, err := pgconn.Connect(context.Background(), connStr)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	return &Replicator{
		conn:          conn,
		relationStore: make(map[uint32]*pglogrepl.RelationMessage),
	}, nil
}

func (r *Replicator) StartReplication(slotName, publication string) error {
	ctx := context.Background()
	r.slotName = slotName
	r.publicationName = publication

	// First, ensure the publication exists
	_, err := r.conn.Exec(ctx, fmt.Sprintf(`
		DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '%s') THEN
				EXECUTE format('CREATE PUBLICATION %I FOR ALL TABLES', '%s');
			END IF;
		END $$;
	`, publication, publication)).ReadAll()
	if err != nil {
		return fmt.Errorf("create publication: %w", err)
	}

	sysident, err := pglogrepl.IdentifySystem(ctx, r.conn)
	if err != nil {
		return fmt.Errorf("identify system: %w", err)
	}
	log.Printf("Connected to Postgres - System ID: %s, Timeline: %d, LSN: %s\n",
		sysident.SystemID, sysident.Timeline, sysident.XLogPos)

	r.lastProcessedLSN = sysident.XLogPos // Initialize LSN

	_, err = pglogrepl.CreateReplicationSlot(ctx, r.conn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Temporary: false,
	})
	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "42710" { // duplicate_object
			log.Printf("Replication slot %s already exists\n", slotName)
		} else {
			return fmt.Errorf("create slot: %w", err)
		}
	}

	pluginArgs := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", publication),
	}
	log.Printf("Starting replication with slot: %s, publication: %s\n", slotName, publication)
	log.Printf("Plugin args: %v\n", pluginArgs)

	err = pglogrepl.StartReplication(ctx, r.conn, slotName, r.lastProcessedLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
	})
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	log.Println("Started logical replication stream...")

	standbyMessageTimeout := 10 * time.Second
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		// Send standby status if the deadline has been reached
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), r.conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: r.lastProcessedLSN,
				WALFlushPosition: r.lastProcessedLSN,
				WALApplyPosition: r.lastProcessedLSN,
				ClientTime:       time.Now(), // <<<< CORRECTED
			})
			if err != nil {
				log.Printf("SendStandbyStatusUpdate (timed) failed: %v", err)
				return fmt.Errorf("send standby status update (timed): %w", err)
			}
			log.Printf("Sent Standby Status Update (timed): LSN %s", r.lastProcessedLSN.String())
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		receiveCtx, cancelReceive := context.WithTimeout(context.Background(), standbyMessageTimeout/2)
		rawMsg, err := r.conn.ReceiveMessage(receiveCtx)
		cancelReceive()

		if err != nil {
			if pgconn.Timeout(err) {
				log.Println("ReceiveMessage timeout, will check for standby message deadline...")
				continue
			}
			log.Printf("ReceiveMessage error: %v", err)
			return fmt.Errorf("receive message: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			log.Printf("Received Postgres error: %+v", errMsg)
			return fmt.Errorf("postgres error: %s", errMsg.Message)
		}

		cd, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("Received unexpected message type: %T msg: %v", rawMsg, rawMsg)
			continue
		}

		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID: // 'k'
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if err != nil {
				log.Printf("ParsePrimaryKeepaliveMessage failed: %v", err)
				return fmt.Errorf("parse primary keepalive: %w", err)
			}
			log.Printf("Primary Keepalive Message => ServerWALEnd:%s ServerTime:%s ReplyRequested:%t", // <<<< Format string %t
				pkm.ServerWALEnd.String(), pkm.ServerTime, pkm.ReplyRequested) // <<<< CORRECTED for log

			if pkm.ReplyRequested  { // <<<< CORRECTED condition
				nextStandbyMessageDeadline = time.Now() // Send immediately or shortly after
				err = pglogrepl.SendStandbyStatusUpdate(context.Background(), r.conn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: r.lastProcessedLSN,
					WALFlushPosition: r.lastProcessedLSN,
					WALApplyPosition: r.lastProcessedLSN,
					ClientTime:       time.Now(), // <<<< CORRECTED
				})
				if err != nil {
					log.Printf("SendStandbyStatusUpdate (keepalive) failed: %v", err)
					return fmt.Errorf("send standby status update (keepalive): %w", err)
				}
				log.Printf("Sent Standby Status Update (reply to keepalive): LSN %s", r.lastProcessedLSN.String())
				nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout) // Reset deadline after sending
			}

		case pglogrepl.XLogDataByteID: // 'w'
			xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
			if err != nil {
				log.Printf("ParseXLogData failed: %v, RawData: %x", err, cd.Data)
				return fmt.Errorf("parse XLogData: %w", err)
			}
			log.Printf("Received XLogData: WALStart %s, ServerWALEnd %s, ServerTime %s",
				xld.WALStart.String(), xld.ServerWALEnd.String(), xld.ServerTime)

			logicalMsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				log.Printf("Logical parse error: %v, WALData: %x", err, xld.WALData)
				continue
			}

			switch msg := logicalMsg.(type) {
			case *pglogrepl.RelationMessage:
				r.relationStore[msg.RelationID] = msg
				log.Printf("Received relation: %s (ID %d)", msg.RelationName, msg.RelationID)
			case *pglogrepl.BeginMessage:
				log.Printf("Begin LSN: %s, CommitTime: %s, Xid: %d", msg.FinalLSN.String(), msg.CommitTime, msg.Xid) // <<<< CORRECTED
			case *pglogrepl.CommitMessage:
				log.Printf("Commit LSN: %s, CommitTime: %s", msg.CommitLSN.String(), msg.CommitTime)
				if msg.CommitLSN > r.lastProcessedLSN {
					r.lastProcessedLSN = msg.CommitLSN
				}
			case *pglogrepl.InsertMessage:
				rel, ok := r.relationStore[msg.RelationID]
				if !ok {
					log.Printf("Unknown relation ID %d for InsertMessage", msg.RelationID)
					continue
				}
				log.Printf("INSERT into %s: %v", rel.RelationName, msg.Tuple.Columns)
				if xld.WALStart > r.lastProcessedLSN {
					r.lastProcessedLSN = xld.WALStart
				}
			case *pglogrepl.UpdateMessage:
				rel, ok := r.relationStore[msg.RelationID]
				if !ok {
					log.Printf("unknown relation ID %d for UpdateMessage", msg.RelationID)
					continue
				}
				log.Printf("UPDATE on %s: %v", rel.RelationName, msg.NewTuple.Columns)
				if xld.WALStart > r.lastProcessedLSN {
					r.lastProcessedLSN = xld.WALStart
				}
			case *pglogrepl.DeleteMessage:
				rel, ok := r.relationStore[msg.RelationID]
				if !ok {
					log.Printf("unknown relation ID %d for DeleteMessage", msg.RelationID)
					continue
				}
				log.Printf("DELETE from %s: %v", rel.RelationName, msg.OldTuple.Columns)
				if xld.WALStart > r.lastProcessedLSN {
					r.lastProcessedLSN = xld.WALStart
				}
			case *pglogrepl.TruncateMessage:
				log.Printf("TRUNCATE on relations: %v", msg.RelationIDs)
				if xld.WALStart > r.lastProcessedLSN {
					r.lastProcessedLSN = xld.WALStart
				}
			default:
				log.Printf("Unhandled logical message type: %T", msg)
			}

			// Send status update after processing XLogData
			// nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout) // Resetting here is also fine
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), r.conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: r.lastProcessedLSN,
				WALFlushPosition: r.lastProcessedLSN,
				WALApplyPosition: r.lastProcessedLSN,
				ClientTime:       time.Now(), // <<<< CORRECTED
			})
			if err != nil {
				log.Printf("SendStandbyStatusUpdate (after XLogData) failed: %v", err)
				return fmt.Errorf("send standby status update (after XLogData): %w", err)
			}
			log.Printf("Sent Standby Status Update (after XLogData): LSN %s", r.lastProcessedLSN.String())
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout) // Ensure deadline is reset

		default:
			log.Printf("Unknown CopyData message type prefix: %X", cd.Data[0])
		}
	}
}