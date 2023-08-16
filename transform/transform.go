package transform

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/pkg/errors"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type DMCreateTable struct {
	SchemaName string
	TableName  string
	CreateStr  string
}

func CreateTableTrans(meta MysqlMeta) (DMCreateTable, error) {
	var err error
	var schemaName string
	var tableName string
	var createSQL string
	var tableComment string
	var primaryDesc string
	var UniqueIndexDesc string
	var IndexDesc string

	var commentAll string

	var dmCreateTable DMCreateTable

	schemaName = meta.Schema
	tableName = meta.TableName
	if meta.Schema == "" {
		createSQL = fmt.Sprintf("CREATE TABLE \"%s\" (", tableName)
	} else {
		createSQL = fmt.Sprintf("CREATE TABLE \"%s\".\"%s\" (", schemaName, tableName)
	}

	//解析字段
	for _, col := range meta.Cols {
		var c1 = fmt.Sprintf("\"%s\"", col.ColumnsName) //列名
		var c2 string                                   // 类型
		var c3 = ""                                     // 自增
		var c4 string                                   // 默认值
		var c5 string                                   // 是否为空
		//var length int
		var colDesc = ""
		// c1 + c2    +  c3              + c4                   +c5
		//"ID"  BIGINT   identity(1,1)   DEFAULT "0"            NOT NULL

		switch strings.ToLower(evaluate(col.ColumnsType)) {
		case "bigint":
			c2 = "bigint"
		case "int":
			c2 = "int"
		case "tinyint":
			c2 = "tinyint"
		case "text":
			c2 = "text"
		case "mediumtext":
			c2 = "text"
		case "longtext":
			c2 = "text"
		case "datetime":
			c2 = "timestamp(0)"
		case "year":
			c2 = "VARCHAR(10)"
		//case "char":
		//if length, err = strconv.Atoi("1"); err != nil {
		//	return dmCreateTable, errors.Wrap(err, "字段长度转化报错")
		//}
		//c2 = fmt.Sprintf("CHAR(%d)", length*3)
		//case "varchar":
		//if length, err = strconv.Atoi("1"); err != nil {
		//	return dmCreateTable, errors.Wrap(err, "字段长度转化报错")
		//}
		//if length*3 > 32500 {
		//	c2 = "text"
		//} else {
		//	c2 = fmt.Sprintf("VARCHAR(%d)", length*3)
		//}
		default:
			c2 = strings.ToLower(col.ColumnsType)
		}
		if col.AutoInc == 1 {
			c3 = `identity(1,1)`
		}

		if col.Default == "" || col.Default == `%!s(<nil>)` {
			c4 = ""
		} else {
			//fmt.Println(col.Default)
			if strings.ToLower(evaluate(col.ColumnsType)) == "varchar" {
				c4 = fmt.Sprintf("default '%s'", col.Default)
			} else if strings.ToLower(evaluate(col.ColumnsType)) == "datetime" {
				c4 = "default CURRENT_TIMESTAMP(0)"
			} else if strings.ToLower(col.ColumnsType) == "bit" {
				// 去除 b 和 '
				c4 = strings.Replace(col.Default, "b", "", -1)
				c4 = "default " + strings.Replace(c4, "'", "", -1)
			} else if strings.ToLower(evaluate(col.ColumnsType)) == "timestamp" {
				if col.Default[0:4] == "0000" {
					c4 = "default 1"
				} else {
					c4 = fmt.Sprintf("default %s", col.Default)
				}
			} else {
				c4 = fmt.Sprintf("default %s", col.Default)
			}
		}

		if col.NotNull == 1 {
			c5 = `not null`
		} else {
			c5 = ``
		}

		if col.Primary == 1 {
			//primaryKeyName = col.ColumnsName
			primaryDesc = fmt.Sprintf("NOT CLUSTER PRIMARY KEY (\"%s\"),", strings.ToLower(col.ColumnsName))
		}

		colDesc = c1 + " " + c2 + " " + c3 + " " + c4 + " " + c5 + ","
		createSQL = StringsADD(createSQL, colDesc)

		if col.Comment != "" {
			afterColumnComment := strings.Replace(col.Comment, ";", "", -1)
			afterColumnComment = strings.Replace(afterColumnComment, "'", "", -1)
			if schemaName == "" {
				commentAll = StringsADD(commentAll, fmt.Sprintf("comment on column \"%s\".\"%s\" IS '%s';", tableName, col.ColumnsName, afterColumnComment))

			} else {
				commentAll = StringsADD(commentAll, fmt.Sprintf("comment on column \"%s\".\"%s\".\"%s\" IS '%s';", schemaName, tableName, col.ColumnsName, afterColumnComment))
			}
		}
	}

	for number, index := range meta.Indexes {
		if index.KeyType == 0 {
			num := len(index.IndexList)
			var indexNameString = "idx"
			var indexColListString = ""
			for i, i2 := range index.IndexList {
				indexNameString = indexNameString + "_" + i2
				indexColListString = indexColListString + fmt.Sprintf("\"%s\" acs", i2)
				if i != num-1 {
					indexColListString = indexColListString + ","
				}
			}
			indexNameString = indexNameString + schemaName + tableName + strconv.Itoa(number) + strconv.Itoa(int(time.Now().UnixNano()))
			//  CREATE INDEX "IDX_XXX" ON "DB_POSEIDON"."CORRECTION"("PROJECT_ID" ASC,"ITEM_ID" ASC) STORAGE(ON "MAIN", CLUSTERBTR);
			if schemaName == "" {
				IndexDesc = StringsADD(IndexDesc, fmt.Sprintf("CREATE INDEX \"%s\" ON \"%s\"(%s) STORAGE(ON \"MAIN\", CLUSTERBTR);", "IDX"+GetMD5Encode(indexNameString), tableName, indexColListString))
			} else {
				IndexDesc = StringsADD(IndexDesc, fmt.Sprintf("CREATE INDEX \"%s\" ON \"%s\".\"%s\"(%s) STORAGE(ON \"MAIN\", CLUSTERBTR);", "IDX"+GetMD5Encode(indexNameString), schemaName, tableName, indexColListString))
			}
		} else if index.KeyType == 1 {
			num := len(index.IndexList)
			// CONSTRAINT "UK_XXX_XXX" UNIQUE("XXX","XXX"),
			var indexNameString = "uk"
			var indexColListString = ""
			for i, i2 := range index.IndexList {
				indexNameString = indexNameString + "_" + i2
				indexColListString = indexColListString + fmt.Sprintf("\"%s\"", i2)
				if i != num-1 {
					indexColListString = indexColListString + ","
				}
			}
			indexNameString = indexNameString + schemaName + tableName + strconv.Itoa(number) + strconv.Itoa(int(time.Now().UnixNano()))
			UniqueIndexDesc = StringsADD(UniqueIndexDesc, fmt.Sprintf("CONSTRAINT \"%s\" UNIQUE(%s),", "UK"+GetMD5Encode(indexNameString), indexColListString))

		} else if index.KeyType == 2 {
			num := len(index.IndexList)
			var indexNameString = ""
			var indexColListString = ""
			for i, i2 := range index.IndexList {
				indexNameString = indexNameString + "_" + i2
				indexColListString = indexColListString + fmt.Sprintf("\"%s\"", i2)
				if i != num-1 {
					indexColListString = indexColListString + ","
				}
			}
			primaryDesc = fmt.Sprintf("NOT CLUSTER PRIMARY KEY (%s),", strings.ToLower(indexColListString))
		}
	}
	if meta.Comment != "" {
		afterTableComment := strings.Replace(meta.Comment, ";", "", -1)
		if schemaName == "" {
			tableComment = fmt.Sprintf("COMMENT ON TABLE \"%s\" IS '%s';", tableName, afterTableComment)
		} else {
			tableComment = fmt.Sprintf("COMMENT ON TABLE \"%s\".\"%s\" IS '%s';", schemaName, tableName, afterTableComment)

		}
	}

	if primaryDesc != "" {
		createSQL = StringsADD(createSQL, primaryDesc)
	}
	if UniqueIndexDesc != "" {
		createSQL = StringsADD(createSQL, UniqueIndexDesc)
	}
	createSQL = strings.TrimRight(createSQL, ",")
	createSQL = createSQL + ") STORAGE(ON \"MAIN\", CLUSTERBTR);"
	if tableComment != "" {
		createSQL = StringsADD(createSQL, tableComment)
	}
	if commentAll != "" {
		createSQL = StringsADD(createSQL, commentAll)
	}
	if IndexDesc != "" {
		createSQL = StringsADD(createSQL, IndexDesc)
	}
	var SQLs string
	SQLs, err = DeleteNullRow(createSQL)
	if err != nil {
		return DMCreateTable{}, errors.Wrap(err, "SQL转化后空行处理失败")
	}
	dmCreateTable.CreateStr = SQLs
	//fmt.Println(SQLs)
	dmCreateTable.TableName = meta.TableName
	dmCreateTable.SchemaName = meta.Schema
	return dmCreateTable, err
}

func evaluate(s string) string {
	result := ""
	for i, j := 0, 0; i < len(s); i++ {
		if s[i] == '(' {
			if j < i {
				result += s[j:i]
			}
			j = i + 1
		} else if s[i] == ')' {
			j = i + 1
			return result
		} else if i == len(s)-1 {
			result += s[j : i+1]
		}
	}
	return result
}

// 去掉括号和括号内的字符
func evaluate_(s string) string {
	result := ""
	for i, j := 0, 0; i < len(s); i++ {
		if s[i] == '(' {
			if j < i {
				result += s[j:i]
			}
			j = i + 1
		} else if s[i] == ')' {
			j = i + 1
		} else if i == len(s)-1 {
			result += s[j : i+1]
		}
	}
	return result
}

func StringsADD(s1 string, s2 string) string {
	return s1 + "\n" + s2
}

// GetMD5Encode 返回一个32位md5加密后的字符串
func GetMD5Encode(data string) string {
	h := md5.New()
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
}

func DeleteNullRow(s1 string) (string, error) {
	regex, err := regexp.Compile("\n\n")
	if err != nil {
		return "", err
	}
	s2 := regex.ReplaceAllString(s1, "\n")
	return s2, nil
}
