#！/bin/bash
 while read line
 do
    #echo $line
    array=(${line//^/ })
    if [ ${#array[@]} -eq 3 ];then
        #echo 'That is ok'
        database=${array[0]}
        echo "当前数据库：${database}"
        table=${array[1]}
        echo "当前数据表：${table}"
        columns=${array[2]}
        echo "当前列：${columns}"
        echo "ending..."
    fi
 done <  a.txt