echo "data pipeline started"

python code/data/cleaning-scripts/metadata.py
echo "metadata.py executed"

python code/data/cleaning-scripts/append.py
echo "append.py executed"

python code/data/cleaning-scripts/add_noon_reps.py
echo "add_noon_reps.py executed"

python code/data/cleaning-scripts/pre_agg_clean.py
echo "pre_agg_clean.py executed"

python code/data/cleaning-scripts/aggregate.py
echo "aggregate.py executed"

echo "data pipeline finished"