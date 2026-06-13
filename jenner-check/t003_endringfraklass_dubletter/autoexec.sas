options obs=100;

/* Materialize the bundled sample change list into the working directory so
   %EndringFraKlass can read it through its FILENAME/INFILE exactly as it would
   read the live Klass API extract. The rows below are the same as input/changes.csv. */
data _null_;
  file "changes.csv";
  put 'sourceCode;sourceName;sourceShortName;targetCode;targetName;targetShortName;changeOccurred';
  put '0301;Oslo;Oslo;0301;Oslo;Oslo;2020-01-01';
  put '5012;Snillfjord;Snillfjord;5059;Orkland;Orkland;2020-01-01';
  put '5012;Snillfjord;Snillfjord;5055;Heim;Heim;2020-01-01';
  put '5012;Snillfjord;Snillfjord;5056;Hitra;Hitra;2020-01-01';
  put '5021;Oppdal;Oppdal;5021;Oppdal;Oppdal;2020-01-01';
  put '1567;Rindal;Rindal;5061;Rindal;Rindal;2020-01-01';
run;
