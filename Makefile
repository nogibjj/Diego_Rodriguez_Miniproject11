install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	python -m pytest -vv --cov=main --cov=mylib test_*.py

format:	
	black *.py 

lint:
	ruff check *.py mylib/*.py

container-lint:
	docker run --rm -i hadolint/hadolint < Dockerfile

refactor: format lint

deploy:
	#deploy goes here
		
all: install lint test format deploy

generate_and_push:
	# Create the markdown file 
	python test_main.py 

	# Add, commit, and push the generated files to GitHub
	@if [ -n "$$(git status --porcelain)" ]; then \
		git config --local user.email "action@github.com"; \
		git config --local user.name "GitHub Action"; \
		git add .; \
		git commit -m "Add SQL log"; \
		git push; \
	else \
		echo "No changes to commit. Skipping commit and push."; \
	fi

extract:
	etl_query extract

transform_load: 
	etl_query transform_load

query:
	etl_query general_query "SELECT t1.country, t1.fertility_rate, t1.viral, AVG(t1.debt) as avg_debt, COUNT(*) as total_countries FROM default.der41_wdi1 t1 JOIN default.der41_wdi2 t2 ON t1.id = t2.id GROUP BY t1.country, t1.fertility_rate, t1.viral ORDER BY total_countries DESC LIMIT 10"

setup_package: 
	python setup.py develop --user