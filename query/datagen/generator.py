import subprocess



#---- dataMode options ----
# pk-fk
# pk-32-fk
# pk-zipf-fk
# pk-4zipf-fk

def joinData ( dataMode ):

    nb = 1000000
    np = 1000000
    alpha = 0.75
    #zn = nb
    zn = 100000

    print ( "Generating join data..." )

    if dataMode == "pk-poisson-fk": 
        cmd = "python3 ../query/datagen/npdist.py poisson"
    else:
        #compile data generator
        cmd = "gcc ../query/datagen/zipfdist.c -lm -std=c99 -o zipfdist"
        output = subprocess.check_output(cmd, shell=True).decode('utf-8')
        print(output, end='')

        cmd = "./zipfdist "
        if dataMode == "pk-fk": 
            cmd += ( "--mode 2 --nbuild " + str(nb) + " --nprobe " + str(np) )

        elif dataMode == "pk-32-fk": 
            cmd += ( "--mode 3 --nbuild " + str(nb) + " --nprobe " + str(np) )
        
        elif dataMode == "pk-8-fk": 
            cmd += ( "--mode 4 --nbuild " + str(nb) + " --nprobe " + str(np) )

        elif dataMode == "pk-zipf-fk": 
            cmd += ( "--mode 1 --nbuild " + str(nb) + " --nprobe " + str(np) + " --zalpha " + str(alpha) + " --zn " + str(zn) + " --zsplit 1" )

        elif dataMode == "pk-4zipf-fk": 
            cmd += ( "--mode 1 --nbuild " + str(nb) + " --nprobe " + str(np) + " --zalpha " + str(alpha) + " --zn " + str(zn) + " --zsplit 4" )
    
    output = subprocess.check_output(cmd, shell=True).decode('utf-8')
    print(output, end='')
    
    print ( "Done" )

