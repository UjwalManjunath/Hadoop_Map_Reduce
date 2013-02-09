Hadoop_Map_Reduce
=================

Large data analysis using hadoop Mapreduce

The Program was run against a huge data file for data analysis. The data file consists of several records
-- each record has 4 fields : (1) Age
                              (2) Gender
                              (3) Program (BS/MS/PHD)
                              (4) GPA
                              
The program calculates Statistics for the GPA for each of the Classifications (by age, by gender, by program)
The following were the statistics calcualted
 * Number of records in each Class
 * Average GPA
 * Maximun GPA
 * Minimum GPA
 * Standard deviaition
                                    
The output would look like:
Statistics based on Age:
Age 15-20:   #of records: XXX      Average GPA: XXX  Maximum GPA:  XXX   Minimum GPA: XXX StdDev.: XXX
Age 21-25:   #of records: XXX      Average GPA: XXX  Maximum GPA:  XXX   Minimum GPA: XXX StdDev.: XXX
Age 26-30:   #of records: XXX      Average GPA: XXX  Maximum GPA:  XXX   Minimum GPA: XXX StdDev.: XXX
Age Higher than 30:  #of records: XXX      Average GPA: XXX  Maximum GPA:  XXX   Minimum GPA: XXX Std. Dev.: XXX

Statistics based on Gender:
Male:     #of records: XXX      Average GPA: XXX  Maximum GPA:  XXX   Minimum GPA: XXX StdDev.:XXX
Female:     #of records: XXX      Average GPA: XXX  Maximum GPA:  XXX   Minimum GPA: XXX StdDev.: XXX

Statistics based on Program:
BS:     #of records: XXX      Average GPA: XXX  Maximum GPA:  XXX   Minimum GPA: XXX StdDev.:XXX
MS:     #of records: XXX      Average GPA: XXX  Maximum GPA:  XXX   Minimum GPA: XXX StdDev.:XXX
PhD:     #of records: XXX      Average GPA: XXX  Maximum GPA:  XXX   Minimum GPA: XXX StdDev.:XXX
