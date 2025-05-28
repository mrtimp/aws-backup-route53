package fileutils

import (
	"bytes"
	"encoding/csv"
	"os"
	"path/filepath"

	"aws-backup-route53/internal/awsutils"
	log "github.com/sirupsen/logrus"
)

func GenerateCSV(accountId string, records []awsutils.Record) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	w := csv.NewWriter(buf)

	if err := w.Write([]string{"Account ID", "Zone ID", "Zone Name", "Record Name", "Record Type", "Record TTL", "Record Value"}); err != nil {
		return nil, err
	}

	for _, record := range records {
		err := w.Write([]string{
			accountId,
			record.ZoneID,
			record.ZoneName,
			record.Name,
			record.Type,
			record.TTL,
			record.Value,
		})
		if err != nil {
			return nil, err
		}
	}

	w.Flush()

	return buf, w.Error()
}

func WriteToFile(path string, buf *bytes.Buffer) error {
	// Ensure parent directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(f)

	_, err = f.WriteString(buf.String())
	return err
}
