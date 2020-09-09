#!/bin/bash
# root qdisc handle
r_handle=1
# netem qdisc handle
n_handle=2 

interface=
delay=
dstip=
qdpresent=
delete=false
verbose=false

error() {
	printf "%s\n" "$1" >&2
}

log() {
	if [ $verbose == true ]; then
		printf "%s" "$1"
	
		if [ "$2" != false ]; then
			printf "\n"
		fi
	fi
}

# Ish...
isip () {
	if ! echo "$1" | grep -E '([0-9]{1,3}[.]){3}[0-9]{1,3}(/[0-9]{1,2})?' > /dev/null; then
		return 1 
	fi
	
	return 0
}

iptohex () {
	printf '%02x' `echo "$1" | sed 's/\./ /g'`
}

usage() {
	echo "$0 -i <interface> -d <ip> -m <milliseconds> [start|stop]"
	exit 1
}

while getopts ":hi:d:m:v" opt; do
	case "${opt}" in
		h)
			usage
			;;

		i)
			interface="${OPTARG}"
			;;

		d)
			dstip="${OPTARG}"
			;;

		m)
			delay="${OPTARG}"
			if ! echo "$delay" | grep -E '^[0-9]+$' > /dev/null; then
				error "-m must be an interger value, got '$delay'"
				usage
			fi
			;;

		v)
			verbose=true
			;;

		*)
			usage
			;;
	esac
done
shift $((OPTIND-1))

if [ "$interface" == '' ]; then
	error "No interface specified"
	usage
fi

if [ "$1" == 'stop' ]; then
	delete=true
elif [ "$1" != 'start' ]; then
	error "Invalid operation '$1', expected start or stop"
	usage
fi

#
#	Play nice with FQDNs too (IPv4 only)
#
if [ "$dstip" != '' ]; then
	if ! isip "$dstip"; then
		ret=`host $dstip`
		rsv=`echo "$ret" | tail -n 1 | grep -o -E '([0-9]{1,3}[.]){3}[0-9]{1,3}'`
		if [ $? -ne 0 ] || ! isip "$rsv"; then
			error "Failed resolving $dstip: $ret"
			exit 1
		fi
		dstip=$rsv
	fi
fi

#
#	Check if we have our queue discipline already added to the target inerface
#	let's hope nothing else if using this handle
#
log "Checking if root qdisc already added to $interface... " false
if tc qdisc show dev "$interface" | grep "qdisc prio $r_handle:" > /dev/null; then
	log "yes"
	qdpresent=true
else
	log "no"
	qdpresent=false
fi

#
#	Were we told to stop delaying packets?
#
if [ $delete == true ]; then
	if [ $qdpresent == true ]; then
		log "Removing qdisc with handle $r_handle... " false
		if tc qdisc del dev "$interface" root handle $r_handle:; then
			log "ok"
		else
			log "failed ($?)"
			exit 1 
		fi
	fi
	exit 0  
fi

#
#	Nope, first add the new root queue discipline if required
#
if [ $qdpresent != true ]; then
	log "Adding qdisc with handle $r_handle... " false
	if tc qdisc add dev "$interface" root handle $r_handle: prio; then
		log "ok"
	else
		log "failed ($?)"
		exit 1
	fi
fi

#
#	Add any IP filters these classify the traffic and limit what's delayed
#
if [ "$dstip" != '' ]; then
	log "Checking if IP is already in filter... " false
	if ! tc filter show dev "$interface" parent $r_handle:0 | grep -E 'match.*'`iptohex "$dstip"` > /dev/null; then
		log "no"

		log "Adding IP $dstip to filter... " false

		# Add a filter to device $interface
		# - attach it to qdisc $r_handle:0
		# - apply it to IP packets
		# - with a prio/pref (priority) of 1 (this is arbitrary as all filters have the same priority)
		# - use the u32 classifier
		# - match on ip dst $dstip
		# - forward matching packets to flowid $n_handle:1
		if tc filter add dev "$interface" parent $r_handle:0 protocol ip prio 1 u32 match ip dst $dstip flowid $n_handle:1; then
			log "ok"
		else
			log "failed ($?)"
			exit 1
		fi
	else
		log "yes"
	fi
fi

#
#	This is the destination for the filters we added above
#
if [ "$delay" != '' ]; then
	log "Checking if netem qdisc has been added (and has correct delay)... " false
	netem=`tc qdisc show dev "$interface" | grep "netem.*$n_handle:"`
	if [ $? -ne 0 ]; then
		log "no"
		log "Adding qdisc netem with handle $n_handle (delay ${delay}ms)... " false
		if tc qdisc add dev "$interface" parent $r_handle:1 handle $n_handle: netem delay ${delay}ms; then
			log "ok"
		else
			log "failed ($?)"
			exit 1
		fi
	elif ! echo "$netem" | grep "delay ${delay}.*ms" > /dev/null; then
		log "yes"
		log "Changing qdisc netem delay to ${delay}ms... " false
		if tc qdisc change dev "$interface" parent $r_handle:1 handle $n_handle: netem delay ${delay}ms; then
			log "ok"
		else
			log "failed ($?)"
			exit 1
		fi
	else
		log "yes"
	fi
fi
