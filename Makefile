# One bash shell per recipe (avoids then/fi errors)
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.ONESHELL:

# Config
YEARS   := 2022 2023
MONTHS  := 01 02 03 04 05 06 07 08 09 10 11 12
BASEURL := https://d37ci6vzurychx.cloudfront.net/trip-data
DATADIR := data
CRASH   := $(DATADIR)/tlc_crashes_monthly_2022_2023.csv
CURLFLAGS := -fL --retry 5 --retry-all-errors --connect-timeout 30

.PHONY: all dirs yellow fhvhv crashes verify clean

all: dirs yellow fhvhv crashes
	@echo "[done] All datasets available in $(DATADIR)/"

dirs:
	mkdir -p "$(DATADIR)"

yellow: dirs
	for Y in $(YEARS); do
	  for M in $(MONTHS); do
	    F="$(DATADIR)/yellow_tripdata_$${Y}-$${M}.parquet"
	    URL="$(BASEURL)/yellow_tripdata_$${Y}-$${M}.parquet"
	    if [[ -s "$$F" ]]; then
	      echo "[skip ] $$F"
	    else
	      echo "[dl   ] $$URL"
	      if ! curl $(CURLFLAGS) -C - -o "$$F" "$$URL"; then
	        echo "[warn ] failed: $$URL"; rm -f "$$F"
	      fi
	    fi
	  done
	done

# NOTE: correct prefix is fhvhv (not hvfhv)
fhvhv: dirs
	for Y in $(YEARS); do
	  for M in $(MONTHS); do
	    F="$(DATADIR)/fhvhv_tripdata_$${Y}-$${M}.parquet"
	    URL="$(BASEURL)/fhvhv_tripdata_$${Y}-$${M}.parquet"
	    if [[ -s "$$F" ]]; then
	      echo "[skip ] $$F"
	    else
	      echo "[dl   ] $$URL"
	      if ! curl $(CURLFLAGS) -C - -o "$$F" "$$URL"; then
	        echo "[warn ] failed: $$URL"; rm -f "$$F"
	      fi
	    fi
	  done
	done
crashes:
	mkdir -p "data"
	if [[ -s "data/tlc_crashes_monthly_2022_2023.csv" ]]; then
		echo "[skip ] data/tlc_crashes_monthly_2022_2023.csv (exists)"
	else
		printf "[csv  ] Fetching crash counts -> %s\n" "data/tlc_crashes_monthly_2022_2023.csv"
		if curl -fL --retry 5 --retry-all-errors --connect-timeout 30 \
			-o "data/tlc_crashes_monthly_2022_2023.csv.tmp" \
			'https://data.cityofnewyork.us/resource/5esv-8c3f.csv?$limit=50000'; then
			mv "data/tlc_crashes_monthly_2022_2023.csv.tmp" "data/tlc_crashes_monthly_2022_2023.csv"
		else
			echo "[warn ] API failed; export CSV manually to data/tlc_crashes_monthly_2022_2023.csv"
			rm -f "data/tlc_crashes_monthly_2022_2023.csv.tmp" || true
		fi
	fi


verify:
	echo "[head ] $(BASEURL)/fhvhv_tripdata_2022-01.parquet"
	curl -I "$(BASEURL)/fhvhv_tripdata_2022-01.parquet" | sed -n '1,10p'
	[[ -s "$(DATADIR)/fhvhv_tripdata_2022-01.parquet" ]] && \
	  echo "[ok   ] sample file exists" || echo "[info ] sample file not downloaded yet"

clean:
	rm -rf "$(DATADIR)"
