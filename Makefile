SHELL := /bin/bash
VENV := .venv/bin
SCOPE ?= demo

.PHONY: setup data pipeline demo dq web hero clean

setup:
	uv venv --python 3.11 .venv
	uv pip install -p .venv "pyspark==3.5.3"
	uv pip install -p .venv -r requirements.txt
	@source scripts/java_env.sh && java -version 2>&1 | head -1
	@$(VENV)/python -c "import pyspark; assert pyspark.__version__=='3.5.3'; print('stack ok')"

data:
	bash scripts/fetch_data.sh

pipeline:
	source scripts/java_env.sh && \
	SCOPE=$(SCOPE) $(VENV)/python src/01_fields.py && \
	SCOPE=$(SCOPE) $(VENV)/python src/02_scenes.py && \
	SCOPE=$(SCOPE) $(VENV)/python src/03_ndvi_zonal.py && \
	SCOPE=$(SCOPE) $(VENV)/python src/05_dq.py

demo: data pipeline

dq:
	source scripts/java_env.sh && SCOPE=$(SCOPE) $(VENV)/python src/05_dq.py

web:
	bash scripts/make_tiles.sh

web-serve:
	bash scripts/serve_web.sh

hero:
	source scripts/java_env.sh && $(VENV)/python scripts/make_hero.py

clean:
	rm -rf warehouse data/tmp
