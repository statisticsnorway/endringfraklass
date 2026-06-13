options obs=100;

/* Materialize the bundled sample change list into the working directory so
   %EndringFraKlass can read it through its FILENAME/INFILE exactly as it would
   read the live Klass API extract. The rows below are the same as input/changes.csv. */
data _null_;
  file "changes.csv";
  put 'sourceCode;sourceName;sourceShortName;targetCode;targetName;targetShortName;changeOccurred';
  put '0101;Halden;Halden;3001;Halden;Halden;2020-01-01';
  put '0104;Moss;Moss;3002;Moss;Moss;2020-01-01';
  put '0105;Sarpsborg;Sarpsborg;3003;Sarpsborg;Sarpsborg;2020-01-01';
  put '0106;Fredrikstad;Fredrikstad;3004;Fredrikstad;Fredrikstad;2020-01-01';
  put '0211;Vestby;Vestby;3019;Vestby;Vestby;2020-01-01';
  put '0213;Ski;Ski;3020;Nordre Follo;NordreFollo;2020-01-01';
  put '0214;Ås;Ås;3021;Ås;Ås;2020-01-01';
  put '1601;Trondheim;Trondheim;5001;Trondheim;Trondheim;2020-01-01';
  put '1702;Steinkjer;Steinkjer;5006;Steinkjer;Steinkjer;2020-01-01';
  put '1804;Bodø;Bodø;1804;Bodø;Bodø;2020-01-01';
run;
