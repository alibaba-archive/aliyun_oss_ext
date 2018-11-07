

# Using default port DEF_PORT

echo base dir is : $PSQLDIR

port=`cat ../../../../config.status |grep DEF_PGPORT\" |awk -F'=' '{print $2}' |awk -F'\"' '{print $2}'`

cp -f ./get_oss_MD5.sql.in ./get_oss_MD5.sql

for line in `cat ./oss.conf.in` ; do

  if [[ $line == "" ]] || [[ $line == "#"* ]]; then
    continue;
  fi

  varname=`echo $line |awk -F'=' '{print $1}'`
  varvalue=`echo $line |awk -F'=' '{print $2}'`

  sed -i "s;@@$varname@@;$varvalue;g" ./get_oss_MD5.sql
done

if [[ -n $PSQLDIR ]] && [[ -e $PSQLDIR ]]; then
  $PSQLDIR/psql -dpostgres -p$port -f ./get_oss_MD5.sql >MD5_data.out
else
  /u01/gpdb/bin/psql -p$port -dpostgres -f ./get_oss_MD5.sql >MD5_data.out 
fi

cp -f ./oss.conf.in ./oss.conf
echo "oss_id_MD5=`cat MD5_data.out |grep MD5 |grep id |sed 's/.*id=\(MD5[0-9a-z]\+\) .*/\1/g'`" >>./oss.conf
echo "oss_key_MD5=`cat MD5_data.out |grep MD5 |grep key |sed 's/.*key=\(MD5[0-9a-z]\+\) .*/\1/g'`" >>./oss.conf

if [[ -d ./input ]] || [[ -d ./output ]]; then
  rm -fr ./input ./output
fi

cp -fr input.in input
cp -fr output.in output

for line in `cat ./oss.conf` ; do

  if [[ $line == "" ]] ; then
    continue;
  fi

  varname=`echo $line |awk -F'=' '{print $1}'`
  varvalue=`echo $line |awk -F'=' '{print $2}'`

  sed -i "s;@@$varname@@;$varvalue;g" ./input/*.source
  sed -i "s;@@$varname@@;$varvalue;g" ./output/*.source

done


