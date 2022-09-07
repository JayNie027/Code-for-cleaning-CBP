clear all 

loc pathJN "C:\Users\niejun\Dropbox\covid"
loc pathJL "C:\Dropbox\Dropbox\covid"
loc path "`pathJN'"
cd "`path'\data\tradeWar\clean"

/// import the CBP_2016
import delimited "`path'\data\tradeWar\CBP\cbp16co.txt "


drop n1_4 n5_9 n10_19 n20_49 n50_99 n100_249 n250_499 n500_999 n1000 n1000_1 ///
 n1000_2 n1000_3 n1000_4
 
drop est ap ap_nf qp1 qp1_nf
 
 
 
 /// extract the numeric part of naics
 destring naics , gen(naics_temp) i(/ -)
 /// keep naics2, naics3, maics4
 tostring naics_temp, replace
 g l=length(naics_temp)
 keep if l<5
 sort fipstate fipscty naics_temp


/// Per Bown et al, CBP does not report employment data for NAICS 111 (\Crop production),
/// NAICS 112 (\Animal production and aquaculture"), and NAICS 113 (\Forestry and logging"),
/// we associate the labor employed in NAICS 1151 to NAICS 111, in NAICS 1152 to NAICS 112, and in NAICS
/// 1153 to NAICS 113
/// However, we can see NAICS 113 in CBP emp data. So, to be consistent across agriculture sectors, really need to take emp from 3-digit NAICS 113 out. 
/// But, complications arise because this could be flagged (i.e. non disclosed).


gen naics2=substr(naics_temp,1,2)
gen naics3=substr(naics_temp,1,3)
gen naics4=substr(naics_temp,1,4)
destring naics2, replace
destring naics3, replace
replace naics3=. if l<3
destring naics4, replace
replace naics4=. if l<4

////////////////////////////////////////////////////////////////////////
//////////////// effective NAICS codes /////////////////////////////////
////////////////////////////////////////////////////////////////////////

gen aux=naics_temp if l==3
destring(aux), replace
bys fipst fipscty naics3: egen naics3eff=max(aux)
tab naics4 if missing(naics3eff) & l==4
drop aux
	* for every fipst-fipscty-naics4 observation, there's also a fipst-fipscty-naics3 observations
	* for the naics3 code of the naics4 code.

gen aux=naics_temp if l==2
destring(aux), replace
bys fipst fipscty naics2: egen naics2eff=max(aux)
tab naics3 if missing(naics2eff) & l==3
drop aux
tab naics2
tab naics_temp if l==2
count if naics2==32 & l==2
count if naics2==33 & l==2
count if naics2==45 & l==2
count if naics2==49 & l==2
	* CBP does not report emp for 2-digit codes 32, 33, 45 and 49. They're 
	* aggregated with other 2-digit NAICS codes

	
foreach n in 11 21 22 23 31 32 33 42 44 45 48 49 51 52 53 54 55 56 61 62 71 72 81 99 {
gen n2`n'=(naics_temp=="`n'")
bys fipst fipscty: egen n2_`n'=max(n2`n')
drop n2`n'
}
replace naics2eff=31 if naics2==32 & missing(naics2eff) & n2_31==1

replace naics2eff=32 if naics2==33 & missing(naics2eff) & n2_32==1
replace naics2eff=31 if naics2==33 & missing(naics2eff) & n2_31==1

replace naics2eff=44 if naics2==45 & missing(naics2eff) & n2_44==1
replace naics2eff=42 if naics2==45 & missing(naics2eff) & n2_42==1

replace naics2eff=48 if naics2==49 & missing(naics2eff) & n2_48==1
replace naics2eff=45 if naics2==49 & missing(naics2eff) & n2_45==1
replace naics2eff=44 if naics2==49 & missing(naics2eff) & n2_44==1
replace naics2eff=42 if naics2==49 & missing(naics2eff) & n2_42==1

drop n2*
////////////////////////////////////////////////////////////////////////



///////////////////////////////////////////////////////////////////////
////// Interpolating flagged employment ///////////////////////////////
///////////////////////////////////////////////////////////////////////

gen empFlagged = 10 if empflag == "A" 
replace empFlagged = 60 if empflag == "B" 
replace empFlagged = 175 if empflag == "C"
replace empFlagged = 375 if empflag == "E"
replace empFlagged = 750 if empflag == "F"
replace empFlagged = 1750 if empflag == "G"
replace empFlagged = 3750 if empflag == "H"
replace empFlagged = 7500 if empflag == "I"
replace empFlagged = 17500 if empflag == "J"
replace empFlagged = 37500 if empflag == "K"
replace empFlagged = 75000 if empflag == "L"


********** 2-digit interpolation ********************
gen emp1nf=emp if l==1 & missing(empflag)
gen emp1f=empFlagged if l==1 & !missing(empflag)
gen emp2nf=emp if l==2 & missing(empflag)
gen emp2f=empFlagged if l==2 & !missing(empflag)


bys fipst fipsc: egen emp1nf_max=max(emp1nf)
bys fipst fipsc: egen emp1f_max=max(emp1f)
bys fipst fipsc: egen emp2nf_tot=total(emp2nf)
bys fipst fipsc: egen emp2f_tot=total(emp2f)

gen emp1resid=emp1nf_max-emp2nf_tot
replace emp1resid=emp1f_max-emp2nf_tot if missing(emp1resid)
	* 3 fipst-fipcty observations have flagged employment at the fipst-fipscty level
	* and every 2-digit naics underneath. So, flagged emp at fipst-fipscty is to be
	* allocated across flagged 2-digit sectors
count if emp2nf_tot>emp1f_max & !missing(emp1f_max)
	* Never happens that emp2nf>emp1f. Hence emp1resid>0 always.
	* That is, 1-digit midpoint flag always bigger than 2-digit disclosed emp

gen weight2=emp2f/emp2f_tot
gen emp2f_w=weight2*emp1resid
	* emp2f_w is interpolated emp2 for 2-digit flagged emp

* check working properly
bys fipst fipsc: egen emp2f_w_tot=total(emp2f_w)
gen auxnf=emp2f_w_tot+emp2nf_tot-emp1nf_max if emp1resid>0
gen auxf=emp2f_w_tot+emp2nf_tot-emp1f_max if emp1resid>0
tab auxf
tab auxnf	
	* rounding error
drop auxf auxnf

* define re-allocated 3-digit employment
gen emp2=emp2f_w if l==2
replace emp2=emp2nf if l==2 & missing(emp2)
	* check that working properly
	bys fipst fipscty: egen emp2t=total(emp2)
	gen aux2=emp2t-emp if l==1
	tab aux2
		* rounding error

********************************************************************


********** 3-digit interpolation ********************
gen emp3nf=emp if l==3 & missing(empflag)
gen emp3f=empFlagged if l==3 & !missing(empflag)
bys fipst fipscty naics2eff: egen aux=max(emp2f_w)
replace emp2f_w=aux
drop aux
	* emp2nf generated above

bys fipst fipsc naics2eff: egen emp2nf_max=max(emp2nf)
bys fipst fipsc naics2eff: egen emp3nf_tot=total(emp3nf)
bys fipst fipsc naics2eff: egen emp3f_tot=total(emp3f)

gen emp2resid=emp2nf_max-emp3nf_tot
replace emp2resid=emp2f_w-emp3nf_tot if missing(emp2resid)
	* for some fipst-fipscty-naicseff2 observations, emp is flagged. So, take 2-digit emp
	* from above step, i.e. emp2f_w, that ensures the sum of all 2-digit emp equals 1-digit emp
count if emp3nf_tot>emp2f_w & !missing(emp2f_w)
	* Never happens that emp3nf_tot>emp2f_w. Hence emp2resid>0 always.
	* That is, 2-digit imputed flag emp always bigger than 3-digit disclosed emp

gen weight3=emp3f/emp3f_tot
gen emp3f_w=weight3*emp2resid
	* emp3f_w is interpolated emp3 for 3-digit flagged emp
	
* check that re-weighting working properly
bys fipst fipsc naics2eff: egen emp3f_w_tot=total(emp3f_w)
gen auxnf=emp3f_w_tot+emp3nf_tot-emp2nf_max if emp2resid>0
gen auxf=emp3f_w_tot+emp3nf_tot-emp2f_w if emp2resid>0
tab auxf if naics_temp!="99"
tab auxnf if naics_temp!="99"
	* these are both just rounding error
	* as seen below, these are all naics2==99 which has no naics3 or naics4 underneath. 
tab naics3 if naics2==99
tab naics4 if naics2==99	
drop auxf auxnf


* check that 3-digit employment adds up to 1-digit employment
gen n99=emp if naics_temp=="99" & missing(empflag)
replace n99=emp2f_w if naics_temp=="99" & !missing(empflag)
bys fipst fipscty: egen n99_max=max(n99)
gen ef3=emp3f_w if l==3
gen enf3=emp3nf if l==3
bys fipst fipscty: egen af3=total(ef3)
bys fipst fipscty: egen anf3=total(enf3)
gen at3=af3+anf3 if l==1 & missing(n99_m)
	* note that 2-digit 99 does not have any 3-digit underneath it
replace at=af+anf+n99_m if l==1 & !missing(n99_m)
gen aux3=at-emp if l==1
tab aux3
bro if aux3>1 & !missing(aux3) & l==1
	* three fipst-fipscty have flagged emp at 1-digit level. These are
	* 15-5, 32-11 and 48-269

* define re-allocated 3-digit employment
gen emp3=emp3f_w if l==3
replace emp3=emp3nf if l==3 & missing(emp3)
	* check that working properly
	bys fipst fipscty: egen emp3t=total(emp3)
	gen aux32=emp3t-emp if missing(n99_m)
	replace aux32=emp3t+n99_m-emp if !missing(n99_m)
	tab aux32 if l==1
		* rounding error
********************************************************************




********** 4-digit interpolation ********************
gen emp4nf=emp if l==4 & missing(empflag)
gen emp4f=empFlagged if l==4 & !missing(empflag)
bys fipst fipscty naics3eff: egen emp3f_w_max=max(emp3f_w)
	* emp3nf generated above

bys fipst fipsc naics3eff: egen emp3nf_max=max(emp3nf)
bys fipst fipsc naics3eff: egen emp4nf_tot=total(emp4nf)
bys fipst fipsc naics3eff: egen emp4f_tot=total(emp4f)

gen emp3resid=emp3nf_max-emp4nf_tot
replace emp3resid=emp3f_w_max-emp4nf_tot if missing(emp3resid)
	* for some fipst-fipscty-naics3eff observations, emp is flagged. So, take 3-digit emp
	* from above step, i.e. emp3f_w, that ensures the sum of all 3-digit emp equals 2-digit emp
count if emp4nf_tot>emp3f_w & !missing(emp3f_w)
	* fipst-fipscty-naics3 1-59-561, 28-119-221, and 48-239-517 have emp4nf>emp3f. This results in emp3resid<0.
	
gen weight4=emp4f/emp4f_tot
gen emp4f_w=weight4*emp3resid
	* emp4f_w is interpolated emp4 for 4-digit flagged emp
	
* check that re-weighting working properly
bys fipst fipsc naics3eff: egen emp4f_w_tot=total(emp4f_w)
gen auxnf=emp4f_w_tot+emp4nf_tot-emp3nf_max if emp3resid>0
gen auxf=emp4f_w_tot+emp4nf_tot-emp3f_w if emp3resid>0
tab auxf if naics_temp!="99"
tab auxnf if naics_temp!="99"
	* these are both just rounding error
tab naics3 if naics2==99
tab naics4 if naics2==99	
*drop auxnf auxf


* check that 4-digit employment adds up to 1-digit employment
gen ef4=emp4f_w if l==4
gen enf4=emp4nf if l==4
bys fipst fipscty: egen af4=total(ef4)
bys fipst fipscty: egen anf4=total(enf4)
gen at4=af4+anf4 if l==1 & missing(n99_m)
	* note that 2-digit 99 does not have any 3-digit underneath it
replace at4=af4+anf4+n99_m if l==1 & !missing(n99_m)
gen aux4=at4-emp if l==1
tab aux4
bro if aux4>1 & !missing(aux4) & l==1
	* three fipst-fipscty have flagged emp at 1-digit level. These are
	* 15-5, 32-11 and 48-269

* define re-allocated 3-digit employment
gen emp4=emp4f_w if l==4
replace emp4=emp4nf if l==4 & missing(emp4)
	* check that working properly
	bys fipst fipscty: egen emp4t=total(emp4)
	gen aux42=emp4t-emp if missing(n99_m)
	replace aux42=emp4t+n99_m-emp if !missing(n99_m)
		* note that 2-digit 99 does not have any 3-digit underneath it
	tab aux42 if l==1
********************************************************************

loc keepVars "fipstate fipscty naics naics_temp empflag emp_nf emp censtate cencty l naics2 naics3 naics4 naics3eff naics2eff empFlagged"
loc keepVars "`keepVars' emp2 emp3 emp4 "
keep `keepVars'

label variable emp "raw CBP data, includes flags"
label variable emp2 "2-digit NAICS CBP data, adjusted so no 2-digit flags. Sum of emp2 = county emp"
label variable emp3 "3-digit NAICS CBP data, adjusted so no 3-digit flags. Sum of emp3 = county emp"
label variable emp4 "4-digit NAICS CBP data, adjusted so no 4-digit flags. Sum of emp4 = county emp"
label variable naics2eff "2-digit NAICS before next 2-digit NAICS code"
label variable naics3eff "3-digit NAICS before next 3-digit NAICS code"
label variable empF "mid-point emp of CBP flag"

********************************************************************************
drop if naics3 == 113

replace naics_temp = "111" if naics_temp == "1151"
replace naics_temp = "112" if naics_temp == "1152"
replace naics_temp = "113" if naics_temp == "1153"

replace emp3 = emp4 if naics_temp == "111"
replace emp3 = emp4 if naics_temp == "112"
replace emp3 = emp4 if naics_temp == "113"

// 115 = 1151 + 1152 + 1153, since move those Ind to 11x, drop 115
drop if naics_temp == "115"
save temp, replace


// deal with sector 99
// sector 99 is the special industry for each county, and it has no naics3
// In TS data, the industry 910, 920, 960 are also the special industries 
// which not shown in any naics industry classification files. So temporaly take thses 
// 3 secotrs as special industry as well.
// Since we plan equally to split the employment in NAICS2 99, so we can aggregate the 910,
// 920, 960 into industry 99 then merge with each other.

drop if l==1
replace naics_temp = "999" if naics_temp == "99"
replace l = length(naics_temp)
keep if l == 3
replace emp3 = emp2 if emp3==.
replace emp3 = round(emp3)

drop naics  empflag emp_nf emp l naics2 naics3 naics4 naics3eff naics2eff empFlagged
drop emp2 emp4

// construct the county emp and industry emp
bys fipstate fipscty: egen emp_county = sum(emp3)
bys naics_temp : egen emp_ind = sum(emp3)

destring naics_temp, replace

// change county 46113 to 46102 in SD
replace fipscty =102 if fipstate == 46 & fipscty == 113

save "cbp16.dta", replace

erase temp.dta
