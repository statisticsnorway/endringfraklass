options obs=100;

/* Materialize the bundled sample change list into the working directory so
   %EndringFraKlass can read it through its FILENAME/INFILE exactly as it would
   read the live Klass API extract. The rows below are the same as input/changes.csv. */
data _null_;
  file "changes.csv";
  put 'sourceCode;sourceName;sourceShortName;targetCode;targetName;targetShortName;changeOccurred';
  put '1567;Rindal;Rindal;1539;Rindal;Rindal;2018-01-01';
  put '1539;Rindal;Rindal;5061;Rindal;Rindal;2020-01-01';
  put '1502;Molde;Molde;1506;Molde;Molde;2020-01-01';
  put '1503;Kristiansund;Kristiansund;1505;Kristiansund;Kristiansund;2020-01-01';
  put '1548;Fræna;Fraena;1579;Hustadvika;Hustadvika;2020-01-01';
  put '1551;Eide;Eide;1579;Hustadvika;Hustadvika;2020-01-01';
run;
