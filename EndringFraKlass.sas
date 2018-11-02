/*
 Program som henter en korrespondanseliste fra Klass og eventuelt eventuelt lager sas-formater for korrespondansen og kodelisten til denne.
 Skrevet av Kristian Lønø
 Versjon 1, 14. mai 2018

 Parametre:
 adresse                 ==> URL-adressen til der Klass ligger. Standard er https://data.ssb.no/api/klass/v1
 klass_katnr             ==> Katalognr fra Klass 
 klass_spraak            ==> Språk, tobokstavskode. Standard: NB (norsk bokmål)
 fra_dato                ==> Fra-dato for gyldighetsperiode. Hvis ikke oppgitt brukes 31.desember 10 år tilbake
 til_dato                ==> Til-dato for gyldighetsperiode. Hvis ikke oppgitt brukes 31.desember ifjor
 utds_endringer          ==> Navn på datasett som vil inneholde endringene. Standard er Klass_endringer
 mappe_format_omk        ==> Navn på mappe der datasettet til omkodingsformat skal lagres. Standard er Work
 utds_format_omk         ==> Navn på datasett som det lages omkodingsformat fra. Standard er Klass_endringer_format
 format_omk              ==> Navn på omkodingsformat som skal lages. Standard er $omkode
 lag_format              ==> 1 hvis det skal lages et format fra kodelisten. Eventuelle dubletter for kode blir slettet før formatet lages.
 format_lib_omk          ==> Navn på mappe der omkodingsformatet skal lagres. Standard er work
 lengde_kode             ==> Lengde for frakoden. Standard er 2
 mappe_dubletter         ==> Navn på mappe til datasett som inneholder dubletter for koder som blir slettet. Standard er work
 utds_dubletter          ==> Navn på datasett som inneholder dubletter for koder som blir slettet. Standard er Klass_endringer_dubl
 liste_gjengangere       ==> 1 hvis gjengangere skal listes for hver runde de oppdages. Standard er 0.
 utds_dubletter_alle     ==> Navn på datasett som inneholder alle dubletter, ikke bare de som blir slettet
 behold_endring_data     ==> 1 hvis datasettet for endringer skal beholdes. Standard er 0.
 behold_format_data      ==> 1 hvis datasett for omkodingsformat skal beholdes. Standard er 1.
 behold_utds_dubletter   ==> 1 hvis datasett med dubletter skal beholdes. Standard er 0.
 slette_midl             ==> 1 hvis midlertidige datasett ønskes slettet
 max_runder              ==> Makismalt antall runder det skal sjekkes for gjengangere. Standard er 5 og det vil normalt være nok

Endringer:
*/


%macro EndringFraKlass
       (
        adresse=https://data.ssb.no/api/klass/v1,
        klass_katnr=131,
        fra_dato=,
        til_dato=,
        klass_spraak=NB,
        mappe_endringer=work,
        utds_endringer=Klass_endringer,
        mappe_format_omk=work,
        utds_format_omk=Klass_endringer_format,
        format_omk=$omkode,
        lag_format_omk=1,
        format_lib_omk=work,
        lengde_kode=22,
        mappe_dubletter=work,
        utds_dubletter=Klass_endringer_dubl,
        list_gjengangere=0,
        behold_endring_data=0,
        behold_format_data=1,
        behold_utds_dubletter=0,
        slette_midl=1,
        max_runder=5
       );
%local endringer_funnet blank_fra_kode blank_til_kode gjenganger runde endringer_dubl;

* Henter 31. desember 10 år tilbake som fra_dato om det ikke er valgt noen fra_dato;
%if "&fra_dato" = "" %then
 %do;
  data _NULL_;
   call symputx('fra_dato',cats(PUT(today(),year4.)-10,'-12-31'),'L');
  run;
  %put NOTE: Fra_dato ikke oppgitt, &fra_dato er valgt;
 %end;

* Henter 31. desember ifjor som til_dato om det ikke er valgt noen til_dato;
%if "&til_dato" = "" %then
 %do;
  data _NULL_;
   call symputx('til_dato',cats(PUT(today(),year4.)-1,'-12-31'),'L');
  run;
  %put NOTE: Til_dato ikke oppgitt, &til_dato er valgt;
 %end;

filename endr url "&adresse./classifications/&klass_katnr./changes?from=&fra_dato.%nrstr(&to)=&til_dato.%nrstr(&csvSeparator)=;%nrstr(&)language=&klass_spraak";

%let endringer_funnet=0;
%let blank_fra_kode=0;
%let blank_til_kode=0;

data &mappe_endringer..&utds_endringer.;
 infile endr dsd dlm=';' truncover firstobs=2;
 input 
  fra_kode        : $&lengde_kode..
  fra_tekst       : $111.
  fra_tekst_kort  : $66.
  til_kode        : $&lengde_kode..
  til_tekst       : $111.
  til_tekst_kort  : $66.
  endringsdato    : yymmdd10.
 ;
 format endringsdato yymmdd10.;
 if fra_kode ne til_kode;
 drop fra_tekst_kort til_tekst_kort;
 if _n_ > 0 then
  call symputx('endringer_funnet',1);
 if fra_kode = '' then
  call symputx('blank_fra_kode',1,'L');
 if til_kode = '' then
  call symputx('blank_til_kode',1,'L');
run;

%if &endringer_funnet. = 1 %then
 %do;

proc sort data=&mappe_endringer..&utds_endringer. out=endringer dupout=&mappe_dubletter..&utds_dubletter nodupkey;
 by fra_kode;
run;

 * Lister de som har blank i fra-kode;
 %if &blank_fra_kode = 1 %then
  %do;
   proc print data=&mappe_endringer..&utds_endringer. n;
    where fra_kode = '';
    title "Fra-kode er blank i katalog &klass_katnr. for perioden &fra_dato - &til_dato..";
   run;
  %end;
 * Lister de som har blank i til-kode;
 %if &blank_til_kode = 1 %then
  %do;
   proc print data=&mappe_endringer..&utds_endringer. n;
    where til_kode = '';
    title "Til-kode er blank i katalog &klass_katnr. for perioden &fra_dato - &til_dato..";
   run;
  %end;

* Lister dubletter om det er noen;
%let endringer_dubl = 0;
 data _null_;
  set &mappe_dubletter..&utds_dubletter (obs=1) nobs=antobs;
  if antobs > 0 then
   call symputx('endringer_dubl',1);
 run;
 %if &endringer_dubl = 1 %then
  %do;
   proc print data=&mappe_dubletter..&utds_dubletter n;
    title "Dubletter i fra_koden for endringer i katalog &klass_katnr. for perioden &fra_dato - &til_dato.. Omkodingsformat lages ikke.";
   run;
  %end;

%put gjenganger=&gjenganger. runde=&runde.;

%if &endringer_dubl ne 1 %then
 %do;
* Finner de som eventuelt har skiftet flere ganger i perioden;
* Henter første og siste kode for perioden. Endringsdato settes til den nyeste;
* Kjøres så lenge det er noen gjengangere;
%let gjenganger = 1;
%let runde = 0;
%do %while (&gjenganger = 1 and &runde. < &max_runder.);
%let runde=%eval(&runde.+1);
proc sql ;
 create table endringer_gjengangere as
 select t2.fra_kode as fra_kode,
        t2.fra_tekst as fra_tekst,
        t1.til_kode as til_kode,
        t1.til_tekst as til_tekst,
        t1.endringsdato as endringsdato
 from endringer as t1 inner join endringer as t2
 on (t1.fra_kode = t2.til_kode)
 where t1.endringsdato > t2.endringsdato
 order by fra_kode, til_kode
 ;
quit;

* Oppdaterer med de som har skiftet flere ganger;
data endringer;
 update endringer endringer_gjengangere;
 by fra_kode ;
run;
%if &list_gjengangere=1 %then
 %do;
  proc print data=endringer_gjengangere n;
   title "Runde &runde..";
  run;
 %end;

* Sjekker om det er flere gjengangere;
%let gjenganger = 0;
 data _null_;
  set endringer_gjengangere (obs=1) nobs=antobs;
  if antobs > 0 then
   call symputx('gjenganger',1);
 run;
%put gjenganger=&gjenganger. runde=&runde.;

%end; /* Slutt Do while */

* Lager omkodingsformat;
%if &lag_format_omk = 1 %then
 %do;
  data &mappe_format_omk..&utds_format_omk.;
   set endringer;
   length start $&lengde_kode. label $&lengde_kode.;
   start = fra_kode;
   label = til_kode;
   fmtname = "&format_omk.";
   drop fra_kode til_kode;
  run;
  proc format cntlin=&mappe_format_omk..&utds_format_omk. lib=&format_lib_omk.;
  run;
 %end;
 
%if &behold_endring_data ne 1 %then
 %do;
  proc datasets lib=&mappe_endringer. nolist;
   delete &utds_endringer. ;
   quit;
 %end;

%if &behold_format_data ne 1 %then
 %do;
  proc datasets lib=&mappe_format_omk. nolist;
   delete &utds_format_omk. ;
   quit;
 %end;

%end; /* Slutt dubletter ikke funnet */

%else
 %do;
  %put WARNING: Dubletter funnet for katalog &klass_katnr. i perioden &fra_dato. - &til_dato.. Omkodingsformat lages ikke.;
 %end;

%if &behold_utds_dubletter ne 1 %then
 %do;
  proc datasets lib=&mappe_dubletter. nolist;
   delete &utds_dubletter. ;
   quit;
 %end;

%if &slette_midl=1 %then
 %do;
  proc datasets lib=work nolist;
   delete endringer endringer_gjengangere  ;
  quit;
 %end;


 %end; /* Slutt endringer_funnet */

%else
 %do;
  %put WARNING: Ingen endringer funnet for katalog &klass_katnr. i perioden &fra_dato. - &til_dato.;
 %end;

%mend;
