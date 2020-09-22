# make sure you have virtualenv installed
# it is recommended to create and use python environment for your every python project
virtualenv env
# activate the env
source env/bin/activate

source .env # credentials


# install requests lib in order to fetch data from webs
# python -m pip install requests
pip install requests
# The psycopg2-binary package makes installation easy
# do not use the binary for production, please install vanilla psycopg2 and a postgres client
# python -m pip install psycopg2-binary
pip install psycopg2-binary
