# Per country download
At present they is an individual way of invoking the relevant download for each country.

## England

1. Follow OS specific instructions to install `node`, `wget` and `python`
    - for MacOS:
```bash
brew install node
```
    - Linux / Windows TBC

2. Install dependencies
```bash
npm install -g mapshaper
pip install -r requirements-non-foss.txt
pip install -r requirements.txt
```

3. Run the download:
```bash
python england.py
```

## Northern Ireland

1. TBC: Install OS specific dependencies; `rust >= 1.72.0 (5680fa18f 2023-08-23)`
`rustup update`

2. Run the test which invokes the download:
```bash
cargo test test_northern_ireland
```

## Singapore
1. Install dependencies
```bash
pip install -r requirements.txt
```

2. Run the downloader
```bash
python singapore/population_by_age_gender.py
```


## Belgium

1. Install dependencies
```bash
pip install -r requirements.txt
```

2. Run the downloader
```bash
python belgium.py
```

## USA

## Australia






