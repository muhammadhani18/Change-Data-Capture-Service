package wal

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings" // Make sure strings is imported
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	// If you decided to use pgx.Identifier, you'd import "github.com/jackc/pgx/v5"
	"github.com/muhammadhani18/go-cdc-service/internal/store"

	"github.com/muhammadhani18/go-cdc-service/internal/kafka"
	"github.com/spf13/viper"
)

// Helper function for basic identifier quoting (use with caution, see notes above)
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}


// ... (Replicator struct and NewReplicator remain the same) ...
type Replicator struct {
	conn             *pgconn.PgConn
	relationStore    map[uint32]*pglogrepl.RelationMessage
	lastProcessedLSN pglogrepl.LSN // To track LSN for StandbyStatusUpdate
	slotName         string
	publicationName  string
	producer         *kafka.Producer
	store 			 *store.Store
}

func NewReplicator(producer *kafka.Producer, s *store.Store) (*Replicator, error) {
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
    
	lastLSN, err := s.GetLSN()
    
	if err != nil {
        return nil, fmt.Errorf("load last LSN: %w", err)
    }
	
	return &Replicator{
		conn:          conn,
		relationStore: make(map[uint32]*pglogrepl.RelationMessage),
		producer:      producer,
		store:            s,
        lastProcessedLSN: lastLSN,	
	}, nil
}

// snapshotTables reads every row from each table and publishes a "snapshot" event.
func (r *Replicator) snapshotTables(ctx context.Context) error {
	tables := viper.GetStringSlice("postgres.tables")
	if len(tables) == 0 {
		log.Println("No tables configured for snapshot, skipping.")
		var lsnStr string
		results, err := r.conn.Exec(ctx, "SELECT pg_current_wal_lsn()").ReadAll()
		if err != nil {
			return fmt.Errorf("fetch current WAL LSN after empty snapshot: %w", err)
		}
		if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
			return fmt.Errorf("no result when fetching current WAL LSN after empty snapshot")
		}
		lsnStr = string(results[0].Rows[0][0])
		lsn, err := pglogrepl.ParseLSN(lsnStr)
		if err != nil {
			return fmt.Errorf("parse LSN %s after empty snapshot: %w", lsnStr, err)
		}
		r.lastProcessedLSN = lsn
		log.Printf("Snapshot skipped (no tables), replication will start from LSN %s", lsn.String())
		return nil
	}

	for _, fqtn := range tables {
		parts := strings.SplitN(fqtn, ".", 2)
		if len(parts) != 2 {
			log.Printf("Invalid table name format in config: %s, skipping", fqtn)
			continue
		}
		schema, table := parts[0], parts[1]

		log.Printf("Snapshotting table: %s.%s", schema, table)
		
		// Using the helper for basic quoting
		query := fmt.Sprintf("SELECT * FROM %s.%s", quoteIdent(schema), quoteIdent(table)) // <<<< CORRECTED
		results, err := r.conn.Exec(ctx, query).ReadAll()
		if err != nil {
			return fmt.Errorf("snapshot select %s: %w", fqtn, err)
		}

		if len(results) == 0 {
			log.Printf("No results from snapshot query for %s, skipping", fqtn)
			continue
		}
		result := results[0]

		var columnNames []string
		var columnDefs []map[string]interface{}
		for _, fd := range result.FieldDescriptions {
			columnNames = append(columnNames, string(fd.Name))
			columnDefs = append(columnDefs, map[string]interface{}{
				"name":         string(fd.Name),
				"table_oid":    fd.TableOID,
				"column_id":    fd.TableAttributeNumber,
				"data_type_oid": fd.DataTypeOID,
				"type_modifier": fd.TypeModifier,
				"format":       fd.Format,
			})
		}

		for _, rawRow := range result.Rows {
			valuesMap := make(map[string]interface{})
			for i, colName := range columnNames {
				if rawRow[i] == nil {
					valuesMap[colName] = nil
				} else {
					valuesMap[colName] = string(rawRow[i]) 
				}
			}

			event := map[string]interface{}{
				"type":      "snapshot",
				"schema":    schema,
				"table":     table,
				"columns":   columnDefs,
				"values":    valuesMap,
				"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
			}
			payload, err := json.Marshal(event)
			if err != nil {
				log.Printf("Marshal snapshot event error for %s: %v, skipping row", fqtn, err)
				continue
			}

			var pkValue interface{}
			if len(rawRow) > 0 && rawRow[0] != nil {
				pkValue = string(rawRow[0]) 
			} else {
				pkValue = "unknown_pk" 
			}
			key := []byte(fmt.Sprintf("%s:%v", table, pkValue))

			if err := r.producer.Publish(ctx, key, payload); err != nil {
				return fmt.Errorf("publish snapshot event for %s: %w", fqtn, err)
			}
			log.Printf("Published snapshot row for %s.%s", schema, table)
		}
	}

	var lsnStr string
	lsnResults, err := r.conn.Exec(ctx, "SELECT pg_current_wal_lsn()").ReadAll()
	if err != nil {
		return fmt.Errorf("fetch current WAL LSN: %w", err)
	}
	if len(lsnResults) == 0 || len(lsnResults[0].Rows) == 0 || len(lsnResults[0].Rows[0]) == 0 {
		return fmt.Errorf("no result when fetching current WAL LSN")
	}
	lsnStr = string(lsnResults[0].Rows[0][0])

	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		return fmt.Errorf("parse LSN %s: %w", lsnStr, err)
	}
	r.lastProcessedLSN = lsn
	log.Printf("Snapshot complete. Replication will start from LSN %s", lsn.String())
	return nil
}

func (r *Replicator) StartReplication(slotName, publication string) error {
	ctx := context.Background()
	r.slotName = slotName
	r.publicationName = publication

	if err := r.snapshotTables(ctx); err != nil {
		return fmt.Errorf("initial snapshot failed: %w", err)
	}

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
	log.Printf("Postgres System ID: %s, Timeline: %d, Initial XLogPos: %s (may be overridden by snapshot LSN)\n",
		sysident.SystemID, sysident.Timeline, sysident.XLogPos)

	log.Printf("Using LSN %s for starting replication (from snapshot or current if snapshot empty/skipped).", r.lastProcessedLSN.String())

	_, err = pglogrepl.CreateReplicationSlot(ctx, r.conn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Temporary: false,
	})
	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "42710" {
			log.Printf("Replication slot %s already exists\n", slotName)
		} else {
			return fmt.Errorf("create slot: %w", err)
		}
	} else {
		log.Printf("Replication slot %s created (or ensured it exists)", slotName)
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
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), r.conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: r.lastProcessedLSN,
				WALFlushPosition: r.lastProcessedLSN,
				WALApplyPosition: r.lastProcessedLSN,
				ClientTime:       time.Now(),
			})
			if err := r.store.SaveLSN(r.lastProcessedLSN); err != nil {
				log.Printf("⚠️ failed to persist LSN %s: %v", r.lastProcessedLSN, err)
			}
			
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
			log.Printf("Received unexpected message type: %T msg: %+v", rawMsg, rawMsg)
			continue
		}

		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if err != nil {
				log.Printf("ParsePrimaryKeepaliveMessage failed: %v", err)
				return fmt.Errorf("parse primary keepalive: %w", err)
			}
			if pkm.ReplyRequested {
				log.Printf("Primary Keepalive requested a reply. Current LSN: %s", r.lastProcessedLSN.String())
				nextStandbyMessageDeadline = time.Now()
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
			if err != nil {
				log.Printf("ParseXLogData failed: %v, RawData: %x", err, cd.Data)
				return fmt.Errorf("parse XLogData: %w", err)
			}
			currentMsgLSN := xld.WALStart
			if currentMsgLSN > r.lastProcessedLSN {
				r.lastProcessedLSN = currentMsgLSN
			}

			logicalMsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				log.Printf("Logical parse error: %v, WALData: %x", err, xld.WALData)
				continue
			}

			switch msg := logicalMsg.(type) {
			case *pglogrepl.RelationMessage:
				r.relationStore[msg.RelationID] = msg
				log.Printf("Relation: %s.%s (ID %d) Columns: %v", msg.Namespace, msg.RelationName, msg.RelationID, msg.Columns)
			case *pglogrepl.BeginMessage:
				log.Printf("Begin: FinalLSN %s, CommitTime %s, Xid %d", msg.FinalLSN.String(), msg.CommitTime, msg.Xid)
			case *pglogrepl.CommitMessage:
				// The Xid of the transaction is in the BeginMessage.
				// CommitMessage has fields like CommitLSN, CommitTime, etc.
				log.Printf("Commit: CommitLSN %s, CommitTime %s, Flags %d", msg.CommitLSN.String(), msg.CommitTime, msg.Flags) // <<<< CORRECTED
				if msg.CommitLSN > r.lastProcessedLSN {
					r.lastProcessedLSN = msg.CommitLSN
				}
			case *pglogrepl.InsertMessage:
				rel, ok := r.relationStore[msg.RelationID]
				if !ok {
					log.Printf("Unknown relation ID %d for InsertMessage, skipping. RelationStore: %v", msg.RelationID, r.relationStore)
					continue
				}
				values := make(map[string]interface{})
				var pkColName string
				if len(rel.Columns) > 0 {
					pkColName = rel.Columns[0].Name // Assuming first column is PK for key
				}

				for i, col := range rel.Columns {
					if i < len(msg.Tuple.Columns) {
						switch msg.Tuple.Columns[i].DataType {
						case 'n': // NULL
							values[col.Name] = nil
						case 'u': // UNCHANGED TOASTed value
							values[col.Name] = "(unchanged)" // Or fetch if needed and possible
						case 't': // TEXT
							values[col.Name] = string(msg.Tuple.Columns[i].Data)
						default: // Binary or other
							values[col.Name] = msg.Tuple.Columns[i].Data // Store as bytes, or decode further
						}
					} else {
						log.Printf("WARN: InsertMessage for %s.%s has fewer tuple columns than relation columns", rel.Namespace, rel.RelationName)
					}
				}
				event := map[string]interface{}{
					"type":      "insert",
					"schema":    rel.Namespace,
					"table":     rel.RelationName,
					"lsn":       xld.WALStart.String(),
					"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
					"data":      values,
				}
				payload, err := json.Marshal(event)
				if err != nil {
					log.Printf("Insert event JSON marshal error: %v", err)
					break
				}

				var kafkaKey string
				if pkColName != "" && values[pkColName] != nil {
					kafkaKey = fmt.Sprintf("%s.%s:%v", rel.Namespace, rel.RelationName, values[pkColName])
				} else {
					kafkaKey = fmt.Sprintf("%s.%s:no_pk_val", rel.Namespace, rel.RelationName) // Fallback key
				}

				if err := r.producer.Publish(context.Background(), []byte(kafkaKey), payload); err != nil {
					log.Printf("Kafka publish error for insert: %v", err)
				} else {
					log.Printf("Published insert to Kafka: %s", kafkaKey)
				}

			case *pglogrepl.UpdateMessage:
				// Similar detailed handling for UpdateMessage
				// You'll have msg.OldTuple (if replica identity allows) and msg.NewTuple
				rel, ok := r.relationStore[msg.RelationID]
				if !ok {
					log.Printf("Unknown relation ID %d for UpdateMessage", msg.RelationID)
					continue
				}
				log.Printf("UPDATE on %s.%s", rel.Namespace, rel.RelationName)
				// ... construct event with old and new data, publish ...

			case *pglogrepl.DeleteMessage:
				// Similar detailed handling for DeleteMessage
				// You'll have msg.OldTuple (if replica identity allows)
				rel, ok := r.relationStore[msg.RelationID]
				if !ok {
					log.Printf("Unknown relation ID %d for DeleteMessage", msg.RelationID)
					continue
				}
				log.Printf("DELETE from %s.%s", rel.Namespace, rel.RelationName)
				// ... construct event with old data, publish ...

			case *pglogrepl.TruncateMessage:
				log.Printf("TRUNCATE on relations: %v", msg.RelationIDs)
				// ... construct event, publish ...

			default:
				log.Printf("Unhandled logical message type: %T, Data: %+v", msg, msg)
			}

			nextStandbyMessageDeadline = time.Now()
		default:
			log.Printf("Unknown CopyData message type prefix: %X (%c)", cd.Data[0], cd.Data[0])
		}
	}
}