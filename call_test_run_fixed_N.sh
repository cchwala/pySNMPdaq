 #!/bin/bash


#N=(10 100 200 300 400 500 600)
#implementations=(0 2 3 4 5)

N=(10 100 200 300 400 500 600 700 800 900)
implementations=(0)


for i in "${N[@]}";
do
    for implementation_id in "${implementations[@]}";
    do
        ./test_run_fixed_N.py $implementation_id $i
    done
done