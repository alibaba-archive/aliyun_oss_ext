
set -x

if [[ -z `which osscmd` ]]; then
  echo "No osscmd found! Can not upload data!"
  exit 1;
fi

for line in `cat ./oss.conf.in` ; do

  if [[ $line == "" ]] ; then
    continue;
  fi

  varname=`echo $line |awk -F'=' '{print $1}'`
  varvalue=`echo $line |awk -F'=' '{print $2}'`

  if [[ $varname == "oss_host" ]]; then
    oss_host=`echo $varvalue |awk -F'/' '{print $3}'` ;
  elif [[ $varname == "oss_id" ]]; then
    oss_id=$varvalue ;
  elif [[ $varname == "oss_key" ]]; then
    oss_key=$varvalue ;
  elif [[ $varname == "oss_bucket" ]]; then
    oss_bucket=$varvalue ;
  fi
done

osscmd config --host=$oss_host --id=$oss_id --key=$oss_key

#cleanup existing dir
osscmd deleteallobject --force=true oss://$oss_bucket/oss_reg_test/
osscmd deleteallobject --force=true oss://$oss_bucket/oss_reg_test2/
osscmd deleteallobject --force=true oss://$oss_bucket/cdn_demo_20170824/
osscmd deleteallobject --force=true oss://$oss_bucket/cdn_demo_201801/
osscmd deleteallobject --force=true oss://$oss_bucket/oss_reg_test/expdir/

#put files. Size of each file should be less than 5G !
for file in `ls ./data` ; do
  osscmd put data/$file oss://$oss_bucket/oss_reg_test/$file
done

osscmd mkdir oss://$oss_bucket/oss_reg_test/expdir/
osscmd mkdir oss://$oss_bucket/oss_reg_test/expdir2/
osscmd mkdir oss://$oss_bucket/oss_reg_test/expdir3/
osscmd mkdir oss://$oss_bucket/oss_reg_test/expdir4/

#put right gz files (endwith 1.gz) to expdir4
for file in `ls ./data|grep "1.gz"` ; do
  osscmd put data/$file oss://$oss_bucket/oss_reg_test/expdir4/$file
done
