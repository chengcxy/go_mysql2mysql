package utils

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"errors"
)



func ParseJsonFile(json_file string)(map[string]interface{},error){
	file,err := os.Open(json_file)
	defer file.Close()
	if err != nil{
		return nil,err
	}
	data,err := ioutil.ReadAll(file)
	if err != nil{
		return nil,err
	}
	var f interface{}
	err = json.Unmarshal(data, &f)
	if err != nil{
		return nil,err
	}
	item := f.(map[string]interface{})
	return item,nil
}



func GenInsertSql(db,table string,fields []string,uniqueindexs[]string,value_list_num int) (string,error){
	//拼接占位符 
	if value_list_num <0{
		return "",errors.New("value_list_num <0 is not allowed")
	}
	uniqueindexs_num := 0
	if uniqueindexs != nil{
		uniqueindexs_num = len(uniqueindexs)
	}

	duplicate_keys := make([]string,0)

	questions := make([]string,len(fields))
	
	for i:=0;i<len(fields);i++{
		questions[i] = "?"
		if uniqueindexs_num !=0 {
			for j:=0;j<uniqueindexs_num;j++{
				if fields[i] != uniqueindexs[j]{
					duplicate_keys = append(duplicate_keys,fmt.Sprintf("%s=values(%s)",fields[i],fields[i]))
				}
			}	
		}else{
			duplicate_keys = append(duplicate_keys,fmt.Sprintf("%s=values(%s)",fields[i],fields[i]))
		}
	}
	questions_str := "(" + strings.Join(questions,",")+")"
	
	all_questions := make([]string,value_list_num/len(fields))
	field_strs := strings.Join(fields,",")
	duplicate_keys_str := strings.Join(duplicate_keys,",")
	for i:=0;i<value_list_num/len(fields);i++{
		all_questions[i] = questions_str
	}
	all_questions_str := strings.Join(all_questions,",")
	sql := fmt.Sprintf("insert into %s.%s(%s)values %s ON DUPLICATE KEY UPDATE %s",db,table,field_strs,all_questions_str,duplicate_keys_str)
	return sql,nil
}