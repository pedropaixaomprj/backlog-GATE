#!/bin/bash
cd ~pentaho/python

git pull origin main
#rm -rf backlog-GATE 
#git clone https://github.com/pedropaixaomprj/backlog-GATE
for file in $(ls ~pentaho/python/backlog-GATE/*.py | sort); do
    # Check if the file exists (in case there are no .py files)
    if [[ -f "$file" ]]; then
        echo "###########################################################"
        echo "Executing $file"
        echo "###########################################################"
        python3.11 "$file"
    else
        echo "No Python files found in the folder."
    fi
done
cd ~pentaho/python/backlog-GATE
git add .
git commit -m "Add backlog and prod report with SAT reference"
git push origin main
