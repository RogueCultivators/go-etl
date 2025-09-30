// Copyright 2020 the go-etl Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jsons

import (
	"encoding/json"
	"log"
	"os"

	"github.com/Breeze0806/go-etl/config"
	"github.com/Breeze0806/go-etl/element"
	"github.com/Breeze0806/go-etl/storage/stream/file"
)

func init() {
	var creator Creator
	file.RegisterCreator("jsons", &creator)
}

type MinimalRecord struct {
	ID   int64  `parquet:"name=id, type=INT64"`                            // 主键
	Data string `parquet:"name=data, type=BYTE_ARRAY, convertedtype=UTF8"` // 完整的JSON数据
}

// Creator 创建parquet输出流的创建器
type Creator struct {
}

// Create 创建名为filename的输出流
func (c *Creator) Create(filename string) (file.OutStream, error) {
	return NewOutStream(filename)
}

// Stream parquet文件流
type Stream struct {
	file *os.File
}

// NewOutStream 创建parquet输出流
func NewOutStream(filename string) (file.OutStream, error) {
	stream := &Stream{}
	var err error
	stream.file, err = os.Create(filename)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

// Writer 创建写入器
func (s *Stream) Writer(conf *config.JSON) (file.StreamWriter, error) {
	return NewWriter(s.file, conf)
}

// Close 关闭输出流
func (s *Stream) Close() (err error) {
	return s.file.Close()
}

// Writer parquet流写入器
type Writer struct {
	file *os.File
}

// NewWriter 创建parquet流写入器
func NewWriter(f *os.File, c *config.JSON) (file.StreamWriter, error) {
	w := &Writer{
		file: f,
	}
	return w, nil
}

// Write 写入记录
func (w *Writer) Write(record element.Record) (err error) {

	d := make(map[string]interface{})
	for i := 0; i < record.ColumnNumber(); i++ {

		col, err := record.GetByIndex(i)
		if err != nil {
			return err
		}
		if col.IsNil() {
			d[col.Name()] = nil
			continue
		}
		switch col.Type() {
		case element.TypeBigInt:
			val, err := col.AsBigInt()
			if err != nil {
				return err
			}
			d[col.Name()], err = val.Int64()
			if err != nil {
				return err
			}
		case element.TypeDecimal:
			val, err := col.AsDecimal()
			if err != nil {
				return err
			}
			d[col.Name()] = val.String()
		case element.TypeBool:
			val, err := col.AsBool()
			if err != nil {
				return err
			}
			d[col.Name()] = val
		case element.TypeString:
			val, err := col.AsString()
			if err != nil {
				return err
			}
			d[col.Name()] = val
		case element.TypeBytes:
			val, err := col.AsBytes()
			if err != nil {
				return err
			}
			d[col.Name()] = val
		case element.TypeTime:
			val, err := col.AsTime()
			if err != nil {
				return err
			}
			d[col.Name()] = val
		default:
			// 默认转换为字符串
			val, err := col.AsString()
			if err != nil {
				return err
			}
			d[col.Name()] = val
		}
	}
	// 转换为JSON
	jsonData, err := json.Marshal(d)
	if err != nil {
		log.Fatal(err)
	}
	_, err = w.file.Write(jsonData)
	_, err = w.file.Write([]byte("\n"))
	// 写入数据
	return err
}

// Flush 刷新至文件
func (w *Writer) Flush() (err error) {
	return
}

// Close 关闭输出流写入器
func (w *Writer) Close() (err error) {
	return w.file.Close()
}
