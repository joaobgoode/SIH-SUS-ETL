all: data/final/final.csv 

data:
	mkdir -p data

data/raw: data
	mkdir -p data/raw

data/processed: data
	mkdir -p data/processed

data/final: data
	mkdir -p data/final

data/processed/codigo_municipio.csv: data/raw data/processed 
	python scripts/cod_municipio.py

data/processed/codigo_procedimento.csv: data/raw data/processed 
	python scripts/cod_procedimento.py

data/processed/sih.csv: data/raw data/processed 
	python scripts/sih.py

data/final/final.csv: data/processed/sih.csv data/processed/codigo_procedimento.csv data/processed/codigo_municipio.csv data/final
	python scripts/process.py

clean:
	rm -rf data/raw/*
	rm -rf data/processed/*
	rm -rf data/final/*
