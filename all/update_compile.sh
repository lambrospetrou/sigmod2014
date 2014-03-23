rm -rf src/backup
mv src src_backup
mv ~/src .
cd src
./compile.sh
cd ..
rm all.tar.gz
tar -cvzf all.tar.gz README run.sh src SUBMISSION_GUIDELINES

