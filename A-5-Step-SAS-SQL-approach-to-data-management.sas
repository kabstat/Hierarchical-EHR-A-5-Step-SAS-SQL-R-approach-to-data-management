
* Created folders in the H directory;
libname EHR "/folders/myfolders/hrd";

FILENAME REFFILE '/folders/myfolders/hrd/doctors.xlsx';

PROC IMPORT DATAFILE=REFFILE
	DBMS=XLSX replace
	OUT=EHR.doctor;
	GETNAMES=YES;
RUN;

PROC CONTENTS DATA=EHR.doctor; RUN;


data doctor;
	set EHR.doctor;
run;

FILENAME REFFILE '/folders/myfolders/hrd/mothers.xlsx';

PROC IMPORT DATAFILE=REFFILE
	DBMS=XLSX replace
	OUT=EHR.mother;
	GETNAMES=YES;
RUN;

PROC CONTENTS DATA=EHR.mother; RUN;



data mother;
	set EHR.mother;
run;


proc sort data = mother; by Mother_ID Sex Race Age Zip;run;
proc sort data = doctor; by Doctor_ID Mother_ID PR1 PR2;run;

title'Data2: Doctors record';
proc print data = doctor (obs = 10);run;
title;

title'Data1: Mothers record';
proc print data = mother (obs = 10);run;
title;

/*********************************************************************/
/*Step 1: Data Pre-processing (checks for duplicate, missing data,…)*/
/*******************************************************************/

/*Base SAS: Doctors dataset*/
/*Eliminate duplicate rows. The nodup option*/
proc sort data = doctor  nodup out= doctor2 dupout = doctor_duplicates;
	by Doctor_ID Mother_ID PR1 PR2;
run;

/*Mothers dataset*/
proc sort data = mother nodup out= mother2 dupout = mother_duplicates;
	by Mother_ID Sex Race Age Zip;
run;

/*Mothers dataset: SAS SQL Approach*/
/*The SQL procedure to select distinct rows*/
proc sql;
	create table SQL_mother as
		select distinct *
			from mother;
quit;
	
proc sql;
	select *
		from SQL_mother;
quit;

/*Doctor*/
proc sql;
	create table SQL_doctor as
		select distinct *
			from doctor;
quit;
	
proc sql;
	select *
		from SQL_doctor;
quit;

/*Generate counts of duplicate datasets*/
/*Mothers dataset*/
proc freq data = mother noprint;
	tables Mother_ID*Sex*Race*Age*Zip
		/ out=newdata;
run;

/*Duplicated records*/
proc sort data = newdata out=sorted; 
	by count ;
		where count > 1;
run;

proc print data = sorted;run;

/*Mothers dataset: SQL approach to count all the duplicate 
  records in all columns of the table */
proc sql;
	create table SQL_Duplicates as
   		select *, count(*) as Count
      		from mother
      			group by Mother_ID, Sex, Race, Age, Zip
      					having count(*) > 1;
quit;

/*QA*/
proc sql;
	select *
		from SQL_Duplicates;
quit;

/****************************/
/*Step 2: Merge both dataset*/
/***************************/
/*Sort both data before merge*/
proc sort data = doctor2; by Mother_ID;run;
proc sort data = mother2; by Mother_ID;run;

data leftjoin rightjoin innerjoin NOmatch_doctor2 NOmatch_mother2 fulljoin NOmatch_in_both;
 	merge doctor2 (IN=In1) mother2 (IN=In2);
 		by Mother_ID;
 	IF In1=1 then output leftjoin; /*all rows in doctor2 are preserved*/
 		IF In2=1 then output rightjoin; /*all rows in mother2 are preserved*/
 	IF (In1=1 and In2=1) then output innerjoin; /*doctor2 are excluded if 
 												they don’t match any rows in mother2,*/
 		IF (In1=0 and In2=1) then output NOmatch_doctor2;
 	IF (In1=1 and In2=0) then output NOmatch_mother2;
 		IF (In1=1 OR In2=1) then output fulljoin;
 	IF (In1+In2)=1 then output NOmatch_in_both;
 run; 
 
 

title'leftjoin snapshot';
proc sort data = leftjoin; by doctor_id;run;
proc print data = leftjoin;run;

proc means data = leftjoin n mean nmiss;
	var age;
run;

title'rightjoin snapshot';
proc print data = rightjoin;run;

proc means data = rightjoin n mean nmiss;
	var age;
run;

 
proc sql;
	create table SQL_leftjoin as 
	select *, coalesce(a.mother_id, b.mother_id) as Mother_ID
		from SQL_doctor as a
			left join SQL_mother as b
				on a.Mother_ID = b.Mother_ID;
quit;
 

title'SQL_leftjoin';
proc sql;
	select count(age) as n, avg(age) as meanAge, nmiss(age) as nmiss
		from SQL_leftjoin;
quit;
 

proc sql;
	create table SQL_rightjoin as 
	select Doctor_ID, coalesce(a.mother_id, b.mother_id) as Mother_ID, 
		 PR1, PR2, Sex, Race, Age, Zip
		from SQL_doctor as a
			right join SQL_mother as b
				on a.Mother_ID = b.Mother_ID;
quit;


title'SQL_rightjoin';
proc sql;
	select count(age) as n, avg(age) as meanAge, nmiss(age) as nmiss
		from SQL_rightjoin;
quit;

/*Check for duplicates in merged datasets*/
proc sort data = leftjoin  nodup out= leftjoin2 dupout = leftjoin_duplicates;
	by Doctor_ID Mother_ID PR1 PR2;
run;

/*******************************************************/
/*Step 3: Create derived variables from merged dataset*/
/*****************************************************/

data dtmgmt_leftjoin;
	set leftjoin;
					/*delivery*/
		if pr1 in (720, 721, 724, 726, 728, 729, 731, 733, 736, 738, 740, 741, 742, 744)
			then pr1_new = 'delivery'; else pr1_new = 'none';
		if pr2 in (720, 721, 724, 726, 728, 729, 731, 733, 736, 738, 740, 741, 742, 744)
			then pr2_new = 'delivery'; else pr2_new = 'none';
		
	if pr1_new = 'delivery' or pr2_new = 'delivery' 
			then delivery = 'yes'; 
				else delivery = 'no ';
			
			delivery_new = (delivery = "yes"); /*Creates delivery dummy variable*/
			
			      /*delivery_csection*/
		if pr1 in (740, 741, 742, 744) then pr1_new_c = 'deliv_csection'; else pr1_new_c = 'none';
		if pr2 in (740, 741, 742, 744) then pr2_new_c = 'deliv_csection'; else pr2_new_c = 'none';

	if pr1_new_c = 'deliv_csection' or pr2_new_c = 'deliv_csection' 
				then delivery_csection = 'yes'; 
					else delivery_csection = 'no ';
		
		c_section = (delivery_csection = "yes"); /*Creates delivery_csection dummy variable*/
					black = (race = "B"); /*Creates dummy variable*/		
run;


/**************************************************/
/*Step 4: QC derived variables from merged dataset*/
/*************************************************/
/*QA: delivery*/
proc freq data = dtmgmt_leftjoin;
	tables delivery*pr1_new*pr2_new
			/list missing;
run;


/*QA: Race*/
proc freq data = dtmgmt_leftjoin;
	tables black*race/list missing;
run;


/*QA: delivery_csection*/
proc freq data = dtmgmt_leftjoin;
	tables delivery_csection*pr1_new_c*pr2_new_c
			/list missing nopercent nofreq;
run;


/*QA: csection*/
proc freq data = dtmgmt_leftjoin;
	tables c_section*delivery_csection
			/list missing nopercent nofreq;
run;

/*QA: delivery and c_section*/
proc freq data = dtmgmt_leftjoin;
	tables delivery*delivery_new*c_section*delivery_csection
			/list missing nopercent nofreq;
run;


/*******************************************************/
/*Step 3: Create derived variables from merged dataset*/
/*****************************************************/
/*Create new derived variables if needed: The SQL procedure */

proc sql;
	create table dtmgmt_SQL_leftjoin as
		select * ,
 			case when pr1 in (720, 721, 724, 726, 728, 729, 731, 733, 
 					736, 738, 740, 741, 742, 744) then "delivery"
 						else "none" end as pr1_new, 
 			case when pr2 in (720, 721, 724, 726, 728, 729, 731, 733, 
 					736, 738, 740, 741, 742, 744) then "delivery"
  						else "none" end as pr2_new, 
       		case when pr1 in (740, 741, 742, 744) then "deliv_csection"
 						else "none" end as pr1_new_c,
 			case when pr2 in (740, 741, 742, 744) then "deliv_csection"
  						else "none" end as pr2_new_c
from SQL_leftjoin;
quit;
proc sql;
	create table dtmgmt_SQL_leftjoin2 as 
	select *,
		case when pr1_new = "delivery" or pr2_new = "delivery" 
				then 'yes' else 'no' end as delivery,
		case when pr2_new_c = "deliv_csection" or pr2_new_c = "deliv_csection" 
				then 'yes' else 'no' end as deliv_csection
      from dtmgmt_SQL_leftjoin;
quit;


proc sql;
	create table dtmgmt_SQL_leftjoin2a as /*Create dummy variables using SQL procedure*/
	select *,
		case when delivery = "yes" then 1 else 0 end as delivery_new,
		case when deliv_csection = "yes" then 1 else 0 end as c_section,
		case when race = "B" then 1 else 0 end as black  
      from dtmgmt_SQL_leftjoin2;
quit;
		

/**************************************************/
/*Step 4: QC derived variables from merged dataset*/
/*************************************************/
/*SQL QA*/
proc sql;
	create table SQL_dtmgmt_QA as
		select  delivery, delivery_new, deliv_csection, c_section, count(*) as count
				from dtmgmt_SQL_leftjoin2a
					group by  delivery, delivery_new, deliv_csection, c_section
						order by  delivery, delivery_new, deliv_csection, c_section;			
quit;
proc print data = SQL_dtmgmt_QA; run;

/******************************************/
/*Step 5: Aggregation of hierarchical data*/
/*****************************************/
/*Base SAS Approach*/

proc sort data = dtmgmt_leftjoin; by Doctor_ID; run;

proc means data = dtmgmt_leftjoin noprint;
	by Doctor_ID; /*Physician level*/
		var delivery_new c_section black;
			output out = dtmgmt_leftjoin2 (drop = _type_ _freq_) 	
				
				sum =   sum_delivery_new /*Total number of deliveries*/
						sum_c_section  /*Total number of csection*/
						sum_black; /*Total no of black patients*/
				
				where delivery = 'yes'; /*only physicians who deliver babies*/
run;

/*QA: Average number of deliveries and c-sections per doctor*/
title'Base SAS';
proc means data=dtmgmt_leftjoin2 mean;
	var sum_delivery_new sum_c_section sum_black;
run;
title;

/******************************************/
/*Step 5: Aggregation of hierarchical data*/
/*****************************************/
/*SAS SQL Approach*/

proc sql;
	create table dtmgmt_SQL_leftjoin2b as 
	select 
		Doctor_ID,	/*Physician level*/
		sum(delivery_new) as Deliveries, /*Total number of deliveries*/
		sum(c_section) as Csections, /*Total number of csection*/
		sum(black) as Blacks /*Total no of black patients*/
				
		from dtmgmt_SQL_leftjoin2a
			where delivery = 'yes'
				group by Doctor_ID;
quit;

/*QA: Average number of deliveries and c-sections per doctor*/
title'SAS SQL';
proc sql;
	select count(*) as physician_count,avg(Deliveries) as mean_delivery_new, 
			avg(Csections) as mean_c_section, avg(Blacks) as mean_black
		from dtmgmt_SQL_leftjoin2b;
quit;
title;

title'Analytical dataset';
proc print data = dtmgmt_SQL_leftjoin2b;
	var Doctor_ID aggregate Deliveries Csections Blacks;
run;
title;

proc tabulate data= dtmgmt_SQL_leftjoin2b ;
	class Doctor_ID / order = freq;
		var  Deliveries Csections Blacks;
			table (Doctor_ID all = 'Total'),  
						Deliveries*Sum=''*f=8.
						Csections*Sum=''*f=8. 
                         Blacks*Sum=''*f=8.;
run;




