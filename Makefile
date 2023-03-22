help:
	echo HELLO WORLD


app:
	cd app && uvicorn _main:app --port 8000 --reload


parsing:
	python -m parsing worker -l warn
