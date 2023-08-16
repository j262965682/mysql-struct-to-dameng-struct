package transform

import (
	"fmt"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/test_driver"
	_ "github.com/pingcap/tidb/parser/test_driver"
	"github.com/pkg/errors"
)

func DDLParse(sql string) (metaList []MysqlMeta, error error) {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, errors.Wrap(err, "解析SQL失败")
	}
	for _, stmt := range stmtNodes {
		switch stmt.(type) {
		case *ast.CreateTableStmt:
			var meta MysqlMeta
			var indexList = make([]*Index, 0)
			var ColList = make([]*Column, 0)
			meta.Cols = ColList
			meta.Indexes = indexList
			//println("------schema")
			//println(stmt.(*ast.CreateTableStmt).Table.Schema.L)
			meta.Schema = stmt.(*ast.CreateTableStmt).Table.Schema.L
			//println("------tableName")
			//println(stmt.(*ast.CreateTableStmt).Table.Name.L)
			meta.TableName = stmt.(*ast.CreateTableStmt).Table.Name.L
			//println("------Comment")
			//fmt.Printf("%+v\n", stmt.(*ast.CreateTableStmt).Options)
			for _, option := range stmt.(*ast.CreateTableStmt).Options {
				//fmt.Printf("%+v", reflect.TypeOf(option))
				if option.Tp == ast.TableOptionComment {
					//fmt.Printf("%+v\n", option.StrValue)
					meta.Comment = option.StrValue
				}
			}
			//fmt.Printf("%+v", stmt.(*ast.CreateTableStmt).Table.TableInfo.Comment)
			//println("------columns")
			for _, col := range stmt.(*ast.CreateTableStmt).Cols {
				var newCol Column
				//fmt.Printf("-colName   %v\n", col.Name.Name.L)
				newCol.ColumnsName = col.Name.Name.L
				//fmt.Printf("-type      %v\n", col.Tp.String())
				newCol.ColumnsType = col.Tp.String()
				for _, option := range col.Options {
					if option.Tp == ast.ColumnOptionNull {
						//fmt.Printf("- null\n")
						newCol.NotNull = 0 //可以为空
					} else if option.Tp == ast.ColumnOptionNotNull {
						//fmt.Printf("- not null\n")
						newCol.NotNull = 1
					} else if option.Tp == ast.ColumnOptionAutoIncrement {
						//fmt.Printf("- AutoIncrement\n")
						newCol.AutoInc = 1
					} else if option.Tp == ast.ColumnOptionPrimaryKey {
						//fmt.Printf("- PrimaryKey\n")
						newCol.Primary = 1
					} else if option.Tp == ast.ColumnOptionDefaultValue {
						switch option.Expr.(type) {
						case *test_driver.ValueExpr:
							//fmt.Printf("-默认值=%v\n", option.Expr.(*test_driver.ValueExpr).GetValue())
							newCol.Default = fmt.Sprintf("%s", option.Expr.(*test_driver.ValueExpr).GetValue())
						case *ast.FuncCallExpr:
							//fmt.Printf("-默认值=%v\n", option.Expr.(*ast.FuncCallExpr).FnName.String())
							newCol.Default = fmt.Sprintf("%s", option.Expr.(*ast.FuncCallExpr).FnName.String())
						}
					} else if option.Tp == ast.ColumnOptionComment {
						switch option.Expr.(type) {
						case ast.ValueExpr:
							//fmt.Printf("-注释=%v\n", option.Expr.(*test_driver.ValueExpr).GetValue())
							newCol.Comment = fmt.Sprintf("%s", option.Expr.(*test_driver.ValueExpr).GetValue())
						}
					}
				}
				ColList = append(ColList, &newCol)
			}
			//println("------indexes")
			for _, constraint := range stmt.(*ast.CreateTableStmt).Constraints {
				var index Index
				//fmt.Printf("%v\n", constraint.Tp)
				if constraint.Tp == ast.ConstraintPrimaryKey {
					//fmt.Printf("-主键索引 %+v\n", constraint.Keys)
					//fmt.Printf("-主键索引\n")
					index.KeyType = 2
					for _, key := range constraint.Keys {
						//fmt.Printf("-索引键 %+v\n", key.Column.Name.L) // 可能多个
						index.IndexList = append(index.IndexList, key.Column.Name.L)
					}
				} else if constraint.Tp == ast.ConstraintUniqKey {
					//fmt.Printf("-唯一索引 %+v\n", constraint)
					//fmt.Printf("-唯一索引\n")
					index.KeyType = 1
					for _, key := range constraint.Keys {
						//fmt.Printf("-索引键 %+v\n", key.Column.Name.L) // 可能多个
						index.IndexList = append(index.IndexList, key.Column.Name.L)
					}
				} else if constraint.Tp == ast.ConstraintIndex {
					//fmt.Printf("-一般索引 %+v\n", constraint)
					//fmt.Printf("-一般索引\n")
					index.KeyType = 0
					for _, key := range constraint.Keys {
						//fmt.Printf("-索引键 %+v\n", key.Column.Name.L) // 可能多个
						index.IndexList = append(index.IndexList, key.Column.Name.L)
					}
				}
				indexList = append(indexList, &index)
			}
			meta.Cols = ColList
			meta.Indexes = indexList
			metaList = append(metaList, meta)
		}
	}
	//fmt.Printf("%+v", metaList)
	return
}

type MysqlMeta struct {
	Schema    string
	TableName string
	Cols      []*Column
	Comment   string
	Indexes   []*Index
}

type Column struct {
	ColumnsName string
	ColumnsType string
	NotNull     int
	AutoInc     int
	Primary     int
	Default     string
	Comment     string
}

type Index struct {
	IndexName string
	IndexList []string
	KeyType   int // 0普通索引  1唯一索引  2主键
}

type ColumnOptionType int

// ColumnOption types.
const (
	ColumnOptionNoOption ColumnOptionType = iota
	ColumnOptionPrimaryKey
	ColumnOptionNotNull
	ColumnOptionAutoIncrement
	ColumnOptionDefaultValue
	ColumnOptionUniqKey
	ColumnOptionNull
	ColumnOptionOnUpdate // For Timestamp and Datetime only.
	ColumnOptionFulltext
	ColumnOptionComment
	ColumnOptionGenerated
	ColumnOptionReference
	ColumnOptionCollate
	ColumnOptionCheck
	ColumnOptionColumnFormat
	ColumnOptionStorage
	ColumnOptionAutoRandom
)

func intToString(typeInt int) string {
	switch typeInt {
	case 0:
		return "NoOption"
	case 1:
		return "PrimaryKey"
	case 2:
		return "NotNull"
	case 3:
		return "AutoIncrement"
	case 4:
		return "DefaultValue"
	case 5:
		return "UniqKey"
	case 6:
		return "Null"
	case 7:
		return "OnUpdate" // For Timestamp and Datetime only.
	case 8:
		return "Fulltext"
	case 9:
		return "Comment"
	case 10:
		return "Generated"
	case 11:
		return "Reference"
	case 12:
		return "Collate"
	case 13:
		return "Check"
	case 14:
		return "ColumnFormat"
	case 15:
		return "Storage"
	case 16:
		return "AutoRandom"
	}
	return ""
}
