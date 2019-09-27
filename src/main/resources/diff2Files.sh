#!/usr/bin/env bash

generatePipes() {
	#loop for dynamic pipes
  number=1
	while :
	do
		if [[ -p "${target}"/oldfilePipe${number} ]] || [[ -p "${target}"/newfilePipe${number} ]]
		then
			number=$(($number + 1))
		else
			mkfifo ${target}/oldfilePipe${number}
			mkfifo ${target}/newfilePipe${number}
			break
		fi
	done
  oldPipe=${target}/oldfilePipe${number}
  newPipe=${target}/newfilePipe${number}
}

fillPipes() {
  oldfile_size=$(stat -c%s "$1")
  newfile_size=$(stat -c%s "$2")

  if [[ ${oldfile_size} -gt 500000 ]] || [[ ${newfile_size} -gt 500000 ]]
  then
    lbzcat $1 | LC_ALL=C sort -u -T ${target} > ${oldPipe} &
    lbzcat $2 | LC_ALL=C sort -u -T ${target} > ${newPipe} &
  else
    lbzcat $1 | LC_ALL=C sort -u > ${oldPipe} &
    lbzcat $2 | LC_ALL=C sort -u > ${newPipe} &
  fi
}

# Syntax: diff2Files [oldFile] [newFile] [target]
if [[ -f $1 ]] && [[ -f $2 ]]
then
  target="$3"
	filename="$(basename $2)"
	noendname="${filename%.*}"
	noendname="${noendname%.*}"
	artifact="${noendname%%_*}"
	cv="${noendname#*_}"
	rawname="${artifact}-diff_${cv}"

	target="$3"

	generatePipes

	fillPipes $1 $2

	#touch file because with no adds _adds.ttl wont be created
	touch ${target}/${rawname}_adds.ttl

	LC_ALL=C comm -3 ${oldPipe} ${newPipe} | awk '/^[\t]/ {print substr($0,2)>"'${target}/${rawname}'_adds.ttl";next} 1' > ${target}/${rawname}_deletes.ttl
	# not using quitdiff for testing/performance issues (yet)
	#quit-diff --diffFormat=eccrev . ${target}/${rawname}_deletes.ttl 1 2 ${target}/${rawname}_adds.ttl > ${target}/${rawname}_eccrev.trig #read parsed files

  # Compressing the diff
  lbzip2 -f ${target}/*.ttl

	rm ${target}/oldfilePipe${number}
	rm ${target}/newfilePipe${number}
	#rm ${target}/${rawname}_adds.ttl
	#rm ${target}/${rawname}_deletes.ttl
fi
