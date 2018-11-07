
#Simple handy script for re-gen a expected file
#Usage: gen_exp.sh xxxxx.sql

port=`cat ../../../../config.status |grep DEF_PGPORT\" |awk -F'=' '{print $2}' |awk -F'\"' '{print $2}'`
basedir=~/tmp_basedir_for_gpdb_bld
MYPWD=`pwd`
MYUSER=$USER

make_exp=true;
su_str=
if [[ "$EUID" == 0 ]];
then
  echo "Running with user $pg_bld_user!"
  su_str="su $pg_bld_user -c "
fi

su_eval() {
  if [[ -z $su_str ]];
  then
    eval "$1"
  else
    eval "$su_str \"$1\" "
  fi
}


su_eval "$basedir/bin/psql -p $port -X -a -q -d postgres -c \"drop database regression\""
su_eval "$basedir/bin/psql -p $port -X -a -q -d postgres -c \"create database regression template template0\""
su_eval "$basedir/bin/psql -p $port -X -a -q -d regression -f $MYPWD/sql/setup_protocol.sql"
su_eval "$basedir/bin/psql -p $port -X -a -q -d postgres -f $MYPWD/sql/setup_tables.sql"

su_eval "$basedir/bin/psql -p $port -X -a -q -d postgres -c \"drop role admin\""
su_eval "$basedir/bin/psql -p $port -X -a -q -d postgres -c \"create user admin superuser login\""

for f in $@
do
echo process file $f

file=$f
filename=`basename $file`

su_eval "$basedir/bin/psql -p $port -X -a -q -d regression -U$MYUSER <$MYPWD/sql/${filename%.*}.sql >$MYPWD/results/${filename%.*}.out 2>&1 "

#replace expected file
if [[ $make_exp == "true" ]];
then
	cp -fr $MYPWD/sql/${filename%.*}.sql $MYPWD/input.in/${filename%.*}.source
	cp -fr $MYPWD/results/${filename%.*}.out $MYPWD/output.in/${filename%.*}.source

	for line in `cat ./oss.conf` ; do
		if [[ $line == "" ]] ; then
		    continue;
		fi

		varname=`echo $line |awk -F'=' '{print $1}'`
		varvalue=`echo $line |awk -F'=' '{print $2}'`

		sed -i "s;$varvalue;@@$varname@@;g" $MYPWD/input.in/*.source
		sed -i "s;$varvalue;@@$varname@@;g" $MYPWD/output.in/*.source

		if [[ $varname == "oss_bucket" ]]; then
		    oss_bucket=$varvalue ;
		fi
	done

fi

ALLTESTS="$ALLTESTS ${filename%.*}"

done

#chown -R postgres $MYPWD/../../../../

osscmd deleteallobject --force=true oss://$oss_bucket/oss_reg_test/expdir/
osscmd deleteallobject --force=true oss://$oss_bucket/oss_reg_test/expdir2/
osscmd deleteallobject --force=true oss://$oss_bucket/oss_reg_test/expdir3/
osscmd deleteallobject --force=true oss://$oss_bucket/oss_reg_test/expdir4/

cd $MYPWD
export PGPORT=$port
export TESTS=$ALLTESTS
export PATH=$basedir/bin:$PATH
make installcheck-tests

