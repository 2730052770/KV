# by YYT

if [ $# -ne 1 ]
then
	echo "use \"./run NUM\""
else
	LAST=`expr $1 - 1`
	for i in `seq 0 $LAST`; do
		./main $i &
		sleep 0.1
	done
fi



