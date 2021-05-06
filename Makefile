s3:
	docker run --name s3server -p 8000:8000 scality/s3server

install:
	pip install -e .[test]

pre-checks-deps: lint-deps
	pip install flake8 mypy_zope "mypy<0.782"

pre-checks: pre-checks-deps
	flake8 guillotina_s3storage --config=setup.cfg
	isort -c -rc guillotina_s3storage
	black --check --verbose guillotina_s3storage
	mypy -p guillotina_s3storage --ignore-missing-imports

lint-deps:
	pip install "isort>=4,<5" black

lint:
	isort -rc guillotina_s3storage
	black guillotina_s3storage


tests: install
	# Run tests
	pytest --capture=no --tb=native -v guillotina_s3storage
