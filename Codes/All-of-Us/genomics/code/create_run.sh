
sed 's/start=0/start=101/g; s/end=101/end=201/g;' sample_snp1.py > sample_snp2.py
sed 's/start=0/start=201/g; s/end=101/end=301/g;' sample_snp1.py > sample_snp3.py
sed 's/start=0/start=301/g; s/end=101/end=401/g;' sample_snp1.py > sample_snp4.py
sed 's/start=0/start=401/g; s/end=101/end=501/g;' sample_snp1.py > sample_snp5.py
sed 's/start=0/start=501/g; s/end=101/end=601/g;' sample_snp1.py > sample_snp6.py
sed 's/start=0/start=601/g; s/end=101/end=701/g;' sample_snp1.py > sample_snp7.py
sed 's/start=0/start=701/g; s/end=101/end=757/g;' sample_snp1.py > sample_snp8.py


time python sample_snp1.py &
time python sample_snp2.py &
time python sample_snp3.py &
time python sample_snp4.py &
time python sample_snp5.py &
time python sample_snp6.py &
time python sample_snp7.py &
time python sample_snp8.py &





