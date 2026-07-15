install:
	python -m pip install -r requirements.txt
	python -m pip install -e .

test:
	python -m unittest discover -s tests -v

update:
	python scripts/update_daily.py

train:
	python scripts/train_models.py

forecast:
	python scripts/run_forecast.py

app:
	streamlit run pages/11_USD_COP.py
