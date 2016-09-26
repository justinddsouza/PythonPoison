CREATE TEMP TABLE $SDM_GROUPER_DXCG_MEDICARE_OUTPUT_STAGE2_$ROMO_$YYYYMM
AS
SELECT
SUBSTRING(ALL_COLUMNS,1,26) AS MEMBER_ID,
TO_DATE(SUBSTRING(ALL_COLUMNS,27,8), 'MMDDYYYY') AS DOB,
TO_NUMBER(SUBSTRING(ALL_COLUMNS,35,3),'99') AS "AGE",
TO_NUMBER(SUBSTRING(ALL_COLUMNS,38,1),'9') AS SEX,
SUBSTRING(ALL_COLUMNS,39,3) AS MED_MBR_MONTHS,
TO_NUMBER(SUBSTRING(ALL_COLUMNS,41,15),'9999999.99') AS EXPEND1,
SUBSTRING(ALL_COLUMNS,55,1) AS MCAID,
SUBSTRING(ALL_COLUMNS,56,1) AS OREC,
SUBSTRING(ALL_COLUMNS,67,2) AS ELIGCAT,
TO_NUMBER(SUBSTRING(ALL_COLUMNS,69,8),'99.99') AS Score_AgeGndY1_Med,
TO_NUMBER(SUBSTRING(ALL_COLUMNS,77,8),'99.99') AS Score_AgeGndY2_Med,
TO_NUMBER(SUBSTRING(ALL_COLUMNS,85,8),'999.999') AS Score_Dxy1,
TO_NUMBER(SUBSTRING(ALL_COLUMNS,93,8),'999.999') AS Score_Dxy2,
TO_NUMBER(CASE WHEN BTRIM(SUBSTRING(ALL_COLUMNS,674,4)) = '' THEN NULL 
		ELSE BTRIM(SUBSTRING(ALL_COLUMNS,674,4)) END,'999') AS Risk_Driver_1,
TO_NUMBER(CASE WHEN BTRIM(SUBSTRING(ALL_COLUMNS,678,6)) = '' THEN NULL 
		ELSE BTRIM(SUBSTRING(ALL_COLUMNS,678,6)) END,'999.99') AS Risk_Driver_1PCT,
TO_NUMBER(CASE WHEN BTRIM(SUBSTRING(ALL_COLUMNS,684,4)) = '' THEN NULL 
		ELSE BTRIM(SUBSTRING(ALL_COLUMNS,684,4)) END,'999') AS Risk_Driver_2,
TO_NUMBER(CASE WHEN BTRIM(SUBSTRING(ALL_COLUMNS,688,6)) = '' THEN NULL 
		ELSE BTRIM(SUBSTRING(ALL_COLUMNS,688,6)) END,'999.99') AS Risk_Driver_2PCT,
TO_NUMBER(CASE WHEN BTRIM(SUBSTRING(ALL_COLUMNS,694,4)) = '' THEN NULL 
		ELSE BTRIM(SUBSTRING(ALL_COLUMNS,694,4)) END,'999') AS Risk_Driver_3,
TO_NUMBER(CASE WHEN BTRIM(SUBSTRING(ALL_COLUMNS,698,6)) = '' THEN NULL 
		ELSE BTRIM(SUBSTRING(ALL_COLUMNS,698,6)) END,'999.99') AS Risk_Driver_3PCT,
TO_NUMBER(CASE WHEN BTRIM(SUBSTRING(ALL_COLUMNS,704,4)) = '' THEN NULL 
		ELSE BTRIM(SUBSTRING(ALL_COLUMNS,704,4)) END,'999') AS Risk_Driver_4,
TO_NUMBER(CASE WHEN BTRIM(SUBSTRING(ALL_COLUMNS,708,6)) = '' THEN NULL 
		ELSE BTRIM(SUBSTRING(ALL_COLUMNS,708,6)) END,'999.99') AS Risk_Driver_4PCT,
TO_NUMBER(CASE WHEN BTRIM(SUBSTRING(ALL_COLUMNS,714,4)) = '' THEN NULL 
		ELSE BTRIM(SUBSTRING(ALL_COLUMNS,714,4)) END,'999') AS Risk_Driver_5,
TO_NUMBER(CASE WHEN BTRIM(SUBSTRING(ALL_COLUMNS,718,6)) = '' THEN NULL 
		ELSE BTRIM(SUBSTRING(ALL_COLUMNS,718,6)) END,'999.99') AS Risk_Driver_5PCT,
BTRIM(SUBSTRING(ALL_COLUMNS,101,5)) AS DCG_Y1,
BTRIM(SUBSTRING(ALL_COLUMNS,107,5)) AS DCG_Y2,
BTRIM(SUBSTRING(ALL_COLUMNS,113,5)) AS ADCG_Y1,
BTRIM(SUBSTRING(ALL_COLUMNS,119,5)) AS ADCG_Y2,
BTRIM(SUBSTRING(ALL_COLUMNS,127,33)) AS ACHCC,
BTRIM(SUBSTRING(ALL_COLUMNS,160,119)) AS RCHCC,
BTRIM(SUBSTRING(ALL_COLUMNS,279,395)) AS CHCC
FROM 
$SDM_GROUPER_DXCG_MEDICARE_OUTPUT_STAGE_$ROMO_$YYYYMM
;


CREATE TABLE $SDM_GROUPER_DXCG_MEDICARE_OUTPUT_STAGE3_$ROMO_$YYYYMM
AS 
SELECT 
MEMBER_ID,
DOB,
"AGE",
SEX,
MED_MBR_MONTHS,
EXPEND1,
MCAID,
OREC,
ELIGCAT,
Score_AgeGndY1_Med,
Score_AgeGndY2_Med,
Score_Dxy1,
Score_Dxy2,
Risk_Driver_1,
Risk_Driver_1PCT,
Risk_Driver_2,
Risk_Driver_2PCT,
Risk_Driver_3,
Risk_Driver_3PCT,
Risk_Driver_4,
Risk_Driver_4PCT,
Risk_Driver_5,
Risk_Driver_5PCT,
DCG_Y1,
DCG_Y2,
ADCG_Y1,
ADCG_Y2,
SUBSTRING(ACHCC,1,1) AS ACHCC001,
SUBSTRING(ACHCC,2,1) AS ACHCC002,
SUBSTRING(ACHCC,3,1) AS ACHCC003,
SUBSTRING(ACHCC,4,1) AS ACHCC004,
SUBSTRING(ACHCC,5,1) AS ACHCC005,
SUBSTRING(ACHCC,6,1) AS ACHCC006,
SUBSTRING(ACHCC,7,1) AS ACHCC007,
SUBSTRING(ACHCC,8,1) AS ACHCC008,
SUBSTRING(ACHCC,9,1) AS ACHCC009,
SUBSTRING(ACHCC,10,1) AS ACHCC010,
SUBSTRING(ACHCC,11,1) AS ACHCC011,
SUBSTRING(ACHCC,12,1) AS ACHCC012,
SUBSTRING(ACHCC,13,1) AS ACHCC013,
SUBSTRING(ACHCC,14,1) AS ACHCC014,
SUBSTRING(ACHCC,15,1) AS ACHCC015,
SUBSTRING(ACHCC,16,1) AS ACHCC016,
SUBSTRING(ACHCC,17,1) AS ACHCC017,
SUBSTRING(ACHCC,18,1) AS ACHCC018,
SUBSTRING(ACHCC,19,1) AS ACHCC019,
SUBSTRING(ACHCC,20,1) AS ACHCC020,
SUBSTRING(ACHCC,21,1) AS ACHCC021,
SUBSTRING(ACHCC,22,1) AS ACHCC022,
SUBSTRING(ACHCC,23,1) AS ACHCC023,
SUBSTRING(ACHCC,24,1) AS ACHCC024,
SUBSTRING(ACHCC,25,1) AS ACHCC025,
SUBSTRING(ACHCC,26,1) AS ACHCC026,
SUBSTRING(ACHCC,27,1) AS ACHCC027,
SUBSTRING(ACHCC,28,1) AS ACHCC028,
SUBSTRING(ACHCC,29,1) AS ACHCC029,
SUBSTRING(ACHCC,30,1) AS ACHCC030,
SUBSTRING(ACHCC,31,1) AS ACHCC031,

SUBSTRING(RCHCC,1,1) AS RCHCC001,
SUBSTRING(RCHCC,2,1) AS RCHCC002,
SUBSTRING(RCHCC,3,1) AS RCHCC003,
SUBSTRING(RCHCC,4,1) AS RCHCC004,
SUBSTRING(RCHCC,5,1) AS RCHCC005,
SUBSTRING(RCHCC,6,1) AS RCHCC006,
SUBSTRING(RCHCC,7,1) AS RCHCC007,
SUBSTRING(RCHCC,8,1) AS RCHCC008,
SUBSTRING(RCHCC,9,1) AS RCHCC009,
SUBSTRING(RCHCC,10,1) AS RCHCC010,
SUBSTRING(RCHCC,11,1) AS RCHCC011,
SUBSTRING(RCHCC,12,1) AS RCHCC012,
SUBSTRING(RCHCC,13,1) AS RCHCC013,
SUBSTRING(RCHCC,14,1) AS RCHCC014,
SUBSTRING(RCHCC,15,1) AS RCHCC015,
SUBSTRING(RCHCC,16,1) AS RCHCC016,
SUBSTRING(RCHCC,17,1) AS RCHCC017,
SUBSTRING(RCHCC,18,1) AS RCHCC018,
SUBSTRING(RCHCC,19,1) AS RCHCC019,
SUBSTRING(RCHCC,20,1) AS RCHCC020,
SUBSTRING(RCHCC,21,1) AS RCHCC021,
SUBSTRING(RCHCC,22,1) AS RCHCC022,
SUBSTRING(RCHCC,23,1) AS RCHCC023,
SUBSTRING(RCHCC,24,1) AS RCHCC024,
SUBSTRING(RCHCC,25,1) AS RCHCC025,
SUBSTRING(RCHCC,26,1) AS RCHCC026,
SUBSTRING(RCHCC,27,1) AS RCHCC027,
SUBSTRING(RCHCC,28,1) AS RCHCC028,
SUBSTRING(RCHCC,29,1) AS RCHCC029,
SUBSTRING(RCHCC,30,1) AS RCHCC030,
SUBSTRING(RCHCC,31,1) AS RCHCC031,
SUBSTRING(RCHCC,32,1) AS RCHCC032,
SUBSTRING(RCHCC,33,1) AS RCHCC033,
SUBSTRING(RCHCC,34,1) AS RCHCC034,
SUBSTRING(RCHCC,35,1) AS RCHCC035,
SUBSTRING(RCHCC,36,1) AS RCHCC036,
SUBSTRING(RCHCC,37,1) AS RCHCC037,
SUBSTRING(RCHCC,38,1) AS RCHCC038,
SUBSTRING(RCHCC,39,1) AS RCHCC039,
SUBSTRING(RCHCC,40,1) AS RCHCC040,
SUBSTRING(RCHCC,41,1) AS RCHCC041,
SUBSTRING(RCHCC,42,1) AS RCHCC042,
SUBSTRING(RCHCC,43,1) AS RCHCC043,
SUBSTRING(RCHCC,44,1) AS RCHCC044,
SUBSTRING(RCHCC,45,1) AS RCHCC045,
SUBSTRING(RCHCC,46,1) AS RCHCC046,
SUBSTRING(RCHCC,47,1) AS RCHCC047,
SUBSTRING(RCHCC,48,1) AS RCHCC048,
SUBSTRING(RCHCC,49,1) AS RCHCC049,
SUBSTRING(RCHCC,50,1) AS RCHCC050,
SUBSTRING(RCHCC,51,1) AS RCHCC051,
SUBSTRING(RCHCC,52,1) AS RCHCC052,
SUBSTRING(RCHCC,53,1) AS RCHCC053,
SUBSTRING(RCHCC,54,1) AS RCHCC054,
SUBSTRING(RCHCC,55,1) AS RCHCC055,
SUBSTRING(RCHCC,56,1) AS RCHCC056,
SUBSTRING(RCHCC,57,1) AS RCHCC057,
SUBSTRING(RCHCC,58,1) AS RCHCC058,
SUBSTRING(RCHCC,59,1) AS RCHCC059,
SUBSTRING(RCHCC,60,1) AS RCHCC060,
SUBSTRING(RCHCC,61,1) AS RCHCC061,
SUBSTRING(RCHCC,62,1) AS RCHCC062,
SUBSTRING(RCHCC,63,1) AS RCHCC063,
SUBSTRING(RCHCC,64,1) AS RCHCC064,
SUBSTRING(RCHCC,65,1) AS RCHCC065,
SUBSTRING(RCHCC,66,1) AS RCHCC066,
SUBSTRING(RCHCC,67,1) AS RCHCC067,
SUBSTRING(RCHCC,68,1) AS RCHCC068,
SUBSTRING(RCHCC,69,1) AS RCHCC069,
SUBSTRING(RCHCC,70,1) AS RCHCC070,
SUBSTRING(RCHCC,71,1) AS RCHCC071,
SUBSTRING(RCHCC,72,1) AS RCHCC072,
SUBSTRING(RCHCC,73,1) AS RCHCC073,
SUBSTRING(RCHCC,74,1) AS RCHCC074,
SUBSTRING(RCHCC,75,1) AS RCHCC075,
SUBSTRING(RCHCC,76,1) AS RCHCC076,
SUBSTRING(RCHCC,77,1) AS RCHCC077,
SUBSTRING(RCHCC,78,1) AS RCHCC078,
SUBSTRING(RCHCC,79,1) AS RCHCC079,
SUBSTRING(RCHCC,80,1) AS RCHCC080,
SUBSTRING(RCHCC,81,1) AS RCHCC081,
SUBSTRING(RCHCC,82,1) AS RCHCC082,
SUBSTRING(RCHCC,83,1) AS RCHCC083,
SUBSTRING(RCHCC,84,1) AS RCHCC084,
SUBSTRING(RCHCC,85,1) AS RCHCC085,
SUBSTRING(RCHCC,86,1) AS RCHCC086,
SUBSTRING(RCHCC,87,1) AS RCHCC087,
SUBSTRING(RCHCC,88,1) AS RCHCC088,
SUBSTRING(RCHCC,89,1) AS RCHCC089,
SUBSTRING(RCHCC,90,1) AS RCHCC090,
SUBSTRING(RCHCC,91,1) AS RCHCC091,
SUBSTRING(RCHCC,92,1) AS RCHCC092,
SUBSTRING(RCHCC,93,1) AS RCHCC093,
SUBSTRING(RCHCC,94,1) AS RCHCC094,
SUBSTRING(RCHCC,95,1) AS RCHCC095,
SUBSTRING(RCHCC,96,1) AS RCHCC096,
SUBSTRING(RCHCC,97,1) AS RCHCC097,
SUBSTRING(RCHCC,98,1) AS RCHCC098,
SUBSTRING(RCHCC,99,1) AS RCHCC099,
SUBSTRING(RCHCC,100,1) AS RCHCC100,
SUBSTRING(RCHCC,101,1) AS RCHCC101,
SUBSTRING(RCHCC,102,1) AS RCHCC102,
SUBSTRING(RCHCC,103,1) AS RCHCC103,
SUBSTRING(RCHCC,104,1) AS RCHCC104,
SUBSTRING(RCHCC,105,1) AS RCHCC105,
SUBSTRING(RCHCC,106,1) AS RCHCC106,
SUBSTRING(RCHCC,107,1) AS RCHCC107,
SUBSTRING(RCHCC,108,1) AS RCHCC108,
SUBSTRING(RCHCC,109,1) AS RCHCC109,
SUBSTRING(RCHCC,110,1) AS RCHCC110,
SUBSTRING(RCHCC,111,1) AS RCHCC111,
SUBSTRING(RCHCC,112,1) AS RCHCC112,
SUBSTRING(RCHCC,113,1) AS RCHCC113,
SUBSTRING(RCHCC,114,1) AS RCHCC114,
SUBSTRING(RCHCC,115,1) AS RCHCC115,
SUBSTRING(RCHCC,116,1) AS RCHCC116,
SUBSTRING(RCHCC,117,1) AS RCHCC117,

SUBSTRING(CHCC,1,1) AS CHCC001,
SUBSTRING(CHCC,2,1) AS CHCC002,
SUBSTRING(CHCC,3,1) AS CHCC003,
SUBSTRING(CHCC,4,1) AS CHCC004,
SUBSTRING(CHCC,5,1) AS CHCC005,
SUBSTRING(CHCC,6,1) AS CHCC006,
SUBSTRING(CHCC,7,1) AS CHCC007,
SUBSTRING(CHCC,8,1) AS CHCC008,
SUBSTRING(CHCC,9,1) AS CHCC009,
SUBSTRING(CHCC,10,1) AS CHCC010,
SUBSTRING(CHCC,11,1) AS CHCC011,
SUBSTRING(CHCC,12,1) AS CHCC012,
SUBSTRING(CHCC,13,1) AS CHCC013,
SUBSTRING(CHCC,14,1) AS CHCC014,
SUBSTRING(CHCC,15,1) AS CHCC015,
SUBSTRING(CHCC,16,1) AS CHCC016,
SUBSTRING(CHCC,17,1) AS CHCC017,
SUBSTRING(CHCC,18,1) AS CHCC018,
SUBSTRING(CHCC,19,1) AS CHCC019,
SUBSTRING(CHCC,20,1) AS CHCC020,
SUBSTRING(CHCC,21,1) AS CHCC021,
SUBSTRING(CHCC,22,1) AS CHCC022,
SUBSTRING(CHCC,23,1) AS CHCC023,
SUBSTRING(CHCC,24,1) AS CHCC024,
SUBSTRING(CHCC,25,1) AS CHCC025,
SUBSTRING(CHCC,26,1) AS CHCC026,
SUBSTRING(CHCC,27,1) AS CHCC027,
SUBSTRING(CHCC,28,1) AS CHCC028,
SUBSTRING(CHCC,29,1) AS CHCC029,
SUBSTRING(CHCC,30,1) AS CHCC030,
SUBSTRING(CHCC,31,1) AS CHCC031,
SUBSTRING(CHCC,32,1) AS CHCC032,
SUBSTRING(CHCC,33,1) AS CHCC033,
SUBSTRING(CHCC,34,1) AS CHCC034,
SUBSTRING(CHCC,35,1) AS CHCC035,
SUBSTRING(CHCC,36,1) AS CHCC036,
SUBSTRING(CHCC,37,1) AS CHCC037,
SUBSTRING(CHCC,38,1) AS CHCC038,
SUBSTRING(CHCC,39,1) AS CHCC039,
SUBSTRING(CHCC,40,1) AS CHCC040,
SUBSTRING(CHCC,41,1) AS CHCC041,
SUBSTRING(CHCC,42,1) AS CHCC042,
SUBSTRING(CHCC,43,1) AS CHCC043,
SUBSTRING(CHCC,44,1) AS CHCC044,
SUBSTRING(CHCC,45,1) AS CHCC045,
SUBSTRING(CHCC,46,1) AS CHCC046,
SUBSTRING(CHCC,47,1) AS CHCC047,
SUBSTRING(CHCC,48,1) AS CHCC048,
SUBSTRING(CHCC,49,1) AS CHCC049,
SUBSTRING(CHCC,50,1) AS CHCC050,
SUBSTRING(CHCC,51,1) AS CHCC051,
SUBSTRING(CHCC,52,1) AS CHCC052,
SUBSTRING(CHCC,53,1) AS CHCC053,
SUBSTRING(CHCC,54,1) AS CHCC054,
SUBSTRING(CHCC,55,1) AS CHCC055,
SUBSTRING(CHCC,56,1) AS CHCC056,
SUBSTRING(CHCC,57,1) AS CHCC057,
SUBSTRING(CHCC,58,1) AS CHCC058,
SUBSTRING(CHCC,59,1) AS CHCC059,
SUBSTRING(CHCC,60,1) AS CHCC060,
SUBSTRING(CHCC,61,1) AS CHCC061,
SUBSTRING(CHCC,62,1) AS CHCC062,
SUBSTRING(CHCC,63,1) AS CHCC063,
SUBSTRING(CHCC,64,1) AS CHCC064,
SUBSTRING(CHCC,65,1) AS CHCC065,
SUBSTRING(CHCC,66,1) AS CHCC066,
SUBSTRING(CHCC,67,1) AS CHCC067,
SUBSTRING(CHCC,68,1) AS CHCC068,
SUBSTRING(CHCC,69,1) AS CHCC069,
SUBSTRING(CHCC,70,1) AS CHCC070,
SUBSTRING(CHCC,71,1) AS CHCC071,
SUBSTRING(CHCC,72,1) AS CHCC072,
SUBSTRING(CHCC,73,1) AS CHCC073,
SUBSTRING(CHCC,74,1) AS CHCC074,
SUBSTRING(CHCC,75,1) AS CHCC075,
SUBSTRING(CHCC,76,1) AS CHCC076,
SUBSTRING(CHCC,77,1) AS CHCC077,
SUBSTRING(CHCC,78,1) AS CHCC078,
SUBSTRING(CHCC,79,1) AS CHCC079,
SUBSTRING(CHCC,80,1) AS CHCC080,
SUBSTRING(CHCC,81,1) AS CHCC081,
SUBSTRING(CHCC,82,1) AS CHCC082,
SUBSTRING(CHCC,83,1) AS CHCC083,
SUBSTRING(CHCC,84,1) AS CHCC084,
SUBSTRING(CHCC,85,1) AS CHCC085,
SUBSTRING(CHCC,86,1) AS CHCC086,
SUBSTRING(CHCC,87,1) AS CHCC087,
SUBSTRING(CHCC,88,1) AS CHCC088,
SUBSTRING(CHCC,89,1) AS CHCC089,
SUBSTRING(CHCC,90,1) AS CHCC090,
SUBSTRING(CHCC,91,1) AS CHCC091,
SUBSTRING(CHCC,92,1) AS CHCC092,
SUBSTRING(CHCC,93,1) AS CHCC093,
SUBSTRING(CHCC,94,1) AS CHCC094,
SUBSTRING(CHCC,95,1) AS CHCC095,
SUBSTRING(CHCC,96,1) AS CHCC096,
SUBSTRING(CHCC,97,1) AS CHCC097,
SUBSTRING(CHCC,98,1) AS CHCC098,
SUBSTRING(CHCC,99,1) AS CHCC099,
SUBSTRING(CHCC,100,1) AS CHCC100,
SUBSTRING(CHCC,101,1) AS CHCC101,
SUBSTRING(CHCC,102,1) AS CHCC102,
SUBSTRING(CHCC,103,1) AS CHCC103,
SUBSTRING(CHCC,104,1) AS CHCC104,
SUBSTRING(CHCC,105,1) AS CHCC105,
SUBSTRING(CHCC,106,1) AS CHCC106,
SUBSTRING(CHCC,107,1) AS CHCC107,
SUBSTRING(CHCC,108,1) AS CHCC108,
SUBSTRING(CHCC,109,1) AS CHCC109,
SUBSTRING(CHCC,110,1) AS CHCC110,
SUBSTRING(CHCC,111,1) AS CHCC111,
SUBSTRING(CHCC,112,1) AS CHCC112,
SUBSTRING(CHCC,113,1) AS CHCC113,
SUBSTRING(CHCC,114,1) AS CHCC114,
SUBSTRING(CHCC,115,1) AS CHCC115,
SUBSTRING(CHCC,116,1) AS CHCC116,
SUBSTRING(CHCC,117,1) AS CHCC117,
SUBSTRING(CHCC,118,1) AS CHCC118,
SUBSTRING(CHCC,119,1) AS CHCC119,
SUBSTRING(CHCC,120,1) AS CHCC120,
SUBSTRING(CHCC,121,1) AS CHCC121,
SUBSTRING(CHCC,122,1) AS CHCC122,
SUBSTRING(CHCC,123,1) AS CHCC123,
SUBSTRING(CHCC,124,1) AS CHCC124,
SUBSTRING(CHCC,125,1) AS CHCC125,
SUBSTRING(CHCC,126,1) AS CHCC126,
SUBSTRING(CHCC,127,1) AS CHCC127,
SUBSTRING(CHCC,128,1) AS CHCC128,
SUBSTRING(CHCC,129,1) AS CHCC129,
SUBSTRING(CHCC,130,1) AS CHCC130,
SUBSTRING(CHCC,131,1) AS CHCC131,
SUBSTRING(CHCC,132,1) AS CHCC132,
SUBSTRING(CHCC,133,1) AS CHCC133,
SUBSTRING(CHCC,134,1) AS CHCC134,
SUBSTRING(CHCC,135,1) AS CHCC135,
SUBSTRING(CHCC,136,1) AS CHCC136,
SUBSTRING(CHCC,137,1) AS CHCC137,
SUBSTRING(CHCC,138,1) AS CHCC138,
SUBSTRING(CHCC,139,1) AS CHCC139,
SUBSTRING(CHCC,140,1) AS CHCC140,
SUBSTRING(CHCC,141,1) AS CHCC141,
SUBSTRING(CHCC,142,1) AS CHCC142,
SUBSTRING(CHCC,143,1) AS CHCC143,
SUBSTRING(CHCC,144,1) AS CHCC144,
SUBSTRING(CHCC,145,1) AS CHCC145,
SUBSTRING(CHCC,146,1) AS CHCC146,
SUBSTRING(CHCC,147,1) AS CHCC147,
SUBSTRING(CHCC,148,1) AS CHCC148,
SUBSTRING(CHCC,149,1) AS CHCC149,
SUBSTRING(CHCC,150,1) AS CHCC150,
SUBSTRING(CHCC,151,1) AS CHCC151,
SUBSTRING(CHCC,152,1) AS CHCC152,
SUBSTRING(CHCC,153,1) AS CHCC153,
SUBSTRING(CHCC,154,1) AS CHCC154,
SUBSTRING(CHCC,155,1) AS CHCC155,
SUBSTRING(CHCC,156,1) AS CHCC156,
SUBSTRING(CHCC,157,1) AS CHCC157,
SUBSTRING(CHCC,158,1) AS CHCC158,
SUBSTRING(CHCC,159,1) AS CHCC159,
SUBSTRING(CHCC,160,1) AS CHCC160,
SUBSTRING(CHCC,161,1) AS CHCC161,
SUBSTRING(CHCC,162,1) AS CHCC162,
SUBSTRING(CHCC,163,1) AS CHCC163,
SUBSTRING(CHCC,164,1) AS CHCC164,
SUBSTRING(CHCC,165,1) AS CHCC165,
SUBSTRING(CHCC,166,1) AS CHCC166,
SUBSTRING(CHCC,167,1) AS CHCC167,
SUBSTRING(CHCC,168,1) AS CHCC168,
SUBSTRING(CHCC,169,1) AS CHCC169,
SUBSTRING(CHCC,170,1) AS CHCC170,
SUBSTRING(CHCC,171,1) AS CHCC171,
SUBSTRING(CHCC,172,1) AS CHCC172,
SUBSTRING(CHCC,173,1) AS CHCC173,
SUBSTRING(CHCC,174,1) AS CHCC174,
SUBSTRING(CHCC,175,1) AS CHCC175,
SUBSTRING(CHCC,176,1) AS CHCC176,
SUBSTRING(CHCC,177,1) AS CHCC177,
SUBSTRING(CHCC,178,1) AS CHCC178,
SUBSTRING(CHCC,179,1) AS CHCC179,
SUBSTRING(CHCC,180,1) AS CHCC180,
SUBSTRING(CHCC,181,1) AS CHCC181,
SUBSTRING(CHCC,182,1) AS CHCC182,
SUBSTRING(CHCC,183,1) AS CHCC183,
SUBSTRING(CHCC,184,1) AS CHCC184,
SUBSTRING(CHCC,185,1) AS CHCC185,
SUBSTRING(CHCC,186,1) AS CHCC186,
SUBSTRING(CHCC,187,1) AS CHCC187,
SUBSTRING(CHCC,188,1) AS CHCC188,
SUBSTRING(CHCC,189,1) AS CHCC189,
SUBSTRING(CHCC,190,1) AS CHCC190,
SUBSTRING(CHCC,191,1) AS CHCC191,
SUBSTRING(CHCC,192,1) AS CHCC192,
SUBSTRING(CHCC,193,1) AS CHCC193,
SUBSTRING(CHCC,194,1) AS CHCC194,
SUBSTRING(CHCC,195,1) AS CHCC195,
SUBSTRING(CHCC,196,1) AS CHCC196,
SUBSTRING(CHCC,197,1) AS CHCC197,
SUBSTRING(CHCC,198,1) AS CHCC198,
SUBSTRING(CHCC,199,1) AS CHCC199,
SUBSTRING(CHCC,200,1) AS CHCC200,
SUBSTRING(CHCC,201,1) AS CHCC201,
SUBSTRING(CHCC,202,1) AS CHCC202,
SUBSTRING(CHCC,203,1) AS CHCC203,
SUBSTRING(CHCC,204,1) AS CHCC204,
SUBSTRING(CHCC,205,1) AS CHCC205,
SUBSTRING(CHCC,206,1) AS CHCC206,
SUBSTRING(CHCC,207,1) AS CHCC207,
SUBSTRING(CHCC,208,1) AS CHCC208,
SUBSTRING(CHCC,209,1) AS CHCC209,
SUBSTRING(CHCC,210,1) AS CHCC210,
SUBSTRING(CHCC,211,1) AS CHCC211,
SUBSTRING(CHCC,212,1) AS CHCC212,
SUBSTRING(CHCC,213,1) AS CHCC213,
SUBSTRING(CHCC,214,1) AS CHCC214,
SUBSTRING(CHCC,215,1) AS CHCC215,
SUBSTRING(CHCC,216,1) AS CHCC216,
SUBSTRING(CHCC,217,1) AS CHCC217,
SUBSTRING(CHCC,218,1) AS CHCC218,
SUBSTRING(CHCC,219,1) AS CHCC219,
SUBSTRING(CHCC,220,1) AS CHCC220,
SUBSTRING(CHCC,221,1) AS CHCC221,
SUBSTRING(CHCC,222,1) AS CHCC222,
SUBSTRING(CHCC,223,1) AS CHCC223,
SUBSTRING(CHCC,224,1) AS CHCC224,
SUBSTRING(CHCC,225,1) AS CHCC225,
SUBSTRING(CHCC,226,1) AS CHCC226,
SUBSTRING(CHCC,227,1) AS CHCC227,
SUBSTRING(CHCC,228,1) AS CHCC228,
SUBSTRING(CHCC,229,1) AS CHCC229,
SUBSTRING(CHCC,230,1) AS CHCC230,
SUBSTRING(CHCC,231,1) AS CHCC231,
SUBSTRING(CHCC,232,1) AS CHCC232,
SUBSTRING(CHCC,233,1) AS CHCC233,
SUBSTRING(CHCC,234,1) AS CHCC234,
SUBSTRING(CHCC,235,1) AS CHCC235,
SUBSTRING(CHCC,236,1) AS CHCC236,
SUBSTRING(CHCC,237,1) AS CHCC237,
SUBSTRING(CHCC,238,1) AS CHCC238,
SUBSTRING(CHCC,239,1) AS CHCC239,
SUBSTRING(CHCC,240,1) AS CHCC240,
SUBSTRING(CHCC,241,1) AS CHCC241,
SUBSTRING(CHCC,242,1) AS CHCC242,
SUBSTRING(CHCC,243,1) AS CHCC243,
SUBSTRING(CHCC,244,1) AS CHCC244,
SUBSTRING(CHCC,245,1) AS CHCC245,
SUBSTRING(CHCC,246,1) AS CHCC246,
SUBSTRING(CHCC,247,1) AS CHCC247,
SUBSTRING(CHCC,248,1) AS CHCC248,
SUBSTRING(CHCC,249,1) AS CHCC249,
SUBSTRING(CHCC,250,1) AS CHCC250,
SUBSTRING(CHCC,251,1) AS CHCC251,
SUBSTRING(CHCC,252,1) AS CHCC252,
SUBSTRING(CHCC,253,1) AS CHCC253,
SUBSTRING(CHCC,254,1) AS CHCC254,
SUBSTRING(CHCC,255,1) AS CHCC255,
SUBSTRING(CHCC,256,1) AS CHCC256,
SUBSTRING(CHCC,257,1) AS CHCC257,
SUBSTRING(CHCC,258,1) AS CHCC258,
SUBSTRING(CHCC,259,1) AS CHCC259,
SUBSTRING(CHCC,260,1) AS CHCC260,
SUBSTRING(CHCC,261,1) AS CHCC261,
SUBSTRING(CHCC,262,1) AS CHCC262,
SUBSTRING(CHCC,263,1) AS CHCC263,
SUBSTRING(CHCC,264,1) AS CHCC264,
SUBSTRING(CHCC,265,1) AS CHCC265,
SUBSTRING(CHCC,266,1) AS CHCC266,
SUBSTRING(CHCC,267,1) AS CHCC267,
SUBSTRING(CHCC,268,1) AS CHCC268,
SUBSTRING(CHCC,269,1) AS CHCC269,
SUBSTRING(CHCC,270,1) AS CHCC270,
SUBSTRING(CHCC,271,1) AS CHCC271,
SUBSTRING(CHCC,272,1) AS CHCC272,
SUBSTRING(CHCC,273,1) AS CHCC273,
SUBSTRING(CHCC,274,1) AS CHCC274,
SUBSTRING(CHCC,275,1) AS CHCC275,
SUBSTRING(CHCC,276,1) AS CHCC276,
SUBSTRING(CHCC,277,1) AS CHCC277,
SUBSTRING(CHCC,278,1) AS CHCC278,
SUBSTRING(CHCC,279,1) AS CHCC279,
SUBSTRING(CHCC,280,1) AS CHCC280,
SUBSTRING(CHCC,281,1) AS CHCC281,
SUBSTRING(CHCC,282,1) AS CHCC282,
SUBSTRING(CHCC,283,1) AS CHCC283,
SUBSTRING(CHCC,284,1) AS CHCC284,
SUBSTRING(CHCC,285,1) AS CHCC285,
SUBSTRING(CHCC,286,1) AS CHCC286,
SUBSTRING(CHCC,287,1) AS CHCC287,
SUBSTRING(CHCC,288,1) AS CHCC288,
SUBSTRING(CHCC,289,1) AS CHCC289,
SUBSTRING(CHCC,290,1) AS CHCC290,
SUBSTRING(CHCC,291,1) AS CHCC291,
SUBSTRING(CHCC,292,1) AS CHCC292,
SUBSTRING(CHCC,293,1) AS CHCC293,
SUBSTRING(CHCC,294,1) AS CHCC294,
SUBSTRING(CHCC,295,1) AS CHCC295,
SUBSTRING(CHCC,296,1) AS CHCC296,
SUBSTRING(CHCC,297,1) AS CHCC297,
SUBSTRING(CHCC,298,1) AS CHCC298,
SUBSTRING(CHCC,299,1) AS CHCC299,
SUBSTRING(CHCC,300,1) AS CHCC300,
SUBSTRING(CHCC,301,1) AS CHCC301,
SUBSTRING(CHCC,302,1) AS CHCC302,
SUBSTRING(CHCC,303,1) AS CHCC303,
SUBSTRING(CHCC,304,1) AS CHCC304,
SUBSTRING(CHCC,305,1) AS CHCC305,
SUBSTRING(CHCC,306,1) AS CHCC306,
SUBSTRING(CHCC,307,1) AS CHCC307,
SUBSTRING(CHCC,308,1) AS CHCC308,
SUBSTRING(CHCC,309,1) AS CHCC309,
SUBSTRING(CHCC,310,1) AS CHCC310,
SUBSTRING(CHCC,311,1) AS CHCC311,
SUBSTRING(CHCC,312,1) AS CHCC312,
SUBSTRING(CHCC,313,1) AS CHCC313,
SUBSTRING(CHCC,314,1) AS CHCC314,
SUBSTRING(CHCC,315,1) AS CHCC315,
SUBSTRING(CHCC,316,1) AS CHCC316,
SUBSTRING(CHCC,317,1) AS CHCC317,
SUBSTRING(CHCC,318,1) AS CHCC318,
SUBSTRING(CHCC,319,1) AS CHCC319,
SUBSTRING(CHCC,320,1) AS CHCC320,
SUBSTRING(CHCC,321,1) AS CHCC321,
SUBSTRING(CHCC,322,1) AS CHCC322,
SUBSTRING(CHCC,323,1) AS CHCC323,
SUBSTRING(CHCC,324,1) AS CHCC324,
SUBSTRING(CHCC,325,1) AS CHCC325,
SUBSTRING(CHCC,326,1) AS CHCC326,
SUBSTRING(CHCC,327,1) AS CHCC327,
SUBSTRING(CHCC,328,1) AS CHCC328,
SUBSTRING(CHCC,329,1) AS CHCC329,
SUBSTRING(CHCC,330,1) AS CHCC330,
SUBSTRING(CHCC,331,1) AS CHCC331,
SUBSTRING(CHCC,332,1) AS CHCC332,
SUBSTRING(CHCC,333,1) AS CHCC333,
SUBSTRING(CHCC,334,1) AS CHCC334,
SUBSTRING(CHCC,335,1) AS CHCC335,
SUBSTRING(CHCC,336,1) AS CHCC336,
SUBSTRING(CHCC,337,1) AS CHCC337,
SUBSTRING(CHCC,338,1) AS CHCC338,
SUBSTRING(CHCC,339,1) AS CHCC339,
SUBSTRING(CHCC,340,1) AS CHCC340,
SUBSTRING(CHCC,341,1) AS CHCC341,
SUBSTRING(CHCC,342,1) AS CHCC342,
SUBSTRING(CHCC,343,1) AS CHCC343,
SUBSTRING(CHCC,344,1) AS CHCC344,
SUBSTRING(CHCC,345,1) AS CHCC345,
SUBSTRING(CHCC,346,1) AS CHCC346,
SUBSTRING(CHCC,347,1) AS CHCC347,
SUBSTRING(CHCC,348,1) AS CHCC348,
SUBSTRING(CHCC,349,1) AS CHCC349,
SUBSTRING(CHCC,350,1) AS CHCC350,
SUBSTRING(CHCC,351,1) AS CHCC351,
SUBSTRING(CHCC,352,1) AS CHCC352,
SUBSTRING(CHCC,353,1) AS CHCC353,
SUBSTRING(CHCC,354,1) AS CHCC354,
SUBSTRING(CHCC,355,1) AS CHCC355,
SUBSTRING(CHCC,356,1) AS CHCC356,
SUBSTRING(CHCC,357,1) AS CHCC357,
SUBSTRING(CHCC,358,1) AS CHCC358,
SUBSTRING(CHCC,359,1) AS CHCC359,
SUBSTRING(CHCC,360,1) AS CHCC360,
SUBSTRING(CHCC,361,1) AS CHCC361,
SUBSTRING(CHCC,362,1) AS CHCC362,
SUBSTRING(CHCC,363,1) AS CHCC363,
SUBSTRING(CHCC,364,1) AS CHCC364,
SUBSTRING(CHCC,365,1) AS CHCC365,
SUBSTRING(CHCC,366,1) AS CHCC366,
SUBSTRING(CHCC,367,1) AS CHCC367,
SUBSTRING(CHCC,368,1) AS CHCC368,
SUBSTRING(CHCC,369,1) AS CHCC369,
SUBSTRING(CHCC,370,1) AS CHCC370,
SUBSTRING(CHCC,371,1) AS CHCC371,
SUBSTRING(CHCC,372,1) AS CHCC372,
SUBSTRING(CHCC,373,1) AS CHCC373,
SUBSTRING(CHCC,374,1) AS CHCC374,
SUBSTRING(CHCC,375,1) AS CHCC375,
SUBSTRING(CHCC,376,1) AS CHCC376,
SUBSTRING(CHCC,377,1) AS CHCC377,
SUBSTRING(CHCC,378,1) AS CHCC378,
SUBSTRING(CHCC,379,1) AS CHCC379,
SUBSTRING(CHCC,380,1) AS CHCC380,
SUBSTRING(CHCC,381,1) AS CHCC381,
SUBSTRING(CHCC,382,1) AS CHCC382,
SUBSTRING(CHCC,383,1) AS CHCC383,
SUBSTRING(CHCC,384,1) AS CHCC384,
SUBSTRING(CHCC,385,1) AS CHCC385,
SUBSTRING(CHCC,386,1) AS CHCC386,
SUBSTRING(CHCC,387,1) AS CHCC387,
SUBSTRING(CHCC,388,1) AS CHCC388,
SUBSTRING(CHCC,389,1) AS CHCC389,
SUBSTRING(CHCC,390,1) AS CHCC390,
SUBSTRING(CHCC,391,1) AS CHCC391,
SUBSTRING(CHCC,392,1) AS CHCC392,
SUBSTRING(CHCC,393,1) AS CHCC393,
SUBSTRING(CHCC,394,1) AS CHCC394
FROM
$SDM_GROUPER_DXCG_MEDICARE_OUTPUT_STAGE2_$ROMO_$YYYYMM
;


ALTER TABLE $SDM_GROUPER_DXCG_MEDICARE_OUTPUT_STAGE3_$ROMO_$YYYYMM
ADD COLUMN C_GROUP VARCHAR(50);

ALTER TABLE $SDM_GROUPER_DXCG_MEDICARE_OUTPUT_STAGE3_$ROMO_$YYYYMM
ADD COLUMN CAT VARCHAR(50);


UPDATE $SDM_GROUPER_DXCG_MEDICARE_OUTPUT_STAGE3_$ROMO_$YYYYMM
SET C_GROUP = 
CASE 
	WHEN ( ACHCC028 = 1 and CHCC359 !=1 )
    THEN 'Neonate'
	WHEN ( ACHCC015 = 1 )
    THEN 'Cardio-Resp Arrest'  
	WHEN ( RCHCC096 = 1 or
             CHCC184 = 1 or
	         CHCC185 = 1 or
	         CHCC332 = 1 or
		     CHCC333 = 1 or
		     CHCC335 = 1 or
			 CHCC336 = 1 or
		     CHCC337 = 1 )
	THEN 'Trauma/Severe Burns'  
    WHEN ( RCHCC014 = 1 or
		     RCHCC019 = 1 or
             CHCC213 = 1 or
			 CHCC214 = 1 or
			 CHCC215 = 1 or
             RCHCC072 = 1 or
             CHCC289 = 1 or
		     CHCC290 = 1 or
             RCHCC108 = 1 or
		     CHCC374 = 1 or
             CHCC380 = 1 )
    THEN 'Transplant'  
    WHEN ( RCHCC086 = 1 or 
            RCHCC087 = 1 )
    THEN 'ESRD/Hemo/Chronic Kidney Disease' 
	WHEN ( ACHCC002 = 1 or 
            RCHCC004 = 1 or
            CHCC033 = 1 or 
			CHCC135 = 1 or
			CHCC137 = 1 or
			CHCC138 = 1 or
			CHCC140 = 1 or
			RCHCC113 = 1 )
    THEN 'Oncology/Hematology'  
	WHEN ( ACHCC017 = 1 and CHCC241 != 1)
	THEN 'Cerebrovascular' 
	WHEN ( RCHCC058 = 1 or
            CHCC212 = 1 or
	        CHCC220 = 1 or
			CHCC222 = 1 or
			CHCC223 = 1 or
			CHCC224 = 1 or
			CHCC226 = 1 or
			RCHCC061 = 1 or
			RCHCC062 = 1 or
            RCHCC069 = 1 or
			CHCC250 = 1 )
	THEN 'Cardiovascular'  
	WHEN ( ACHCC004 = 1 or
            CHCC269=1 or
            CHCC270=1 )
	THEN 'Diabetes'   
	WHEN ( CHCC259 = 1 or
	        CHCC260 = 1 or
			CHCC261 = 1 or
			CHCC264 = 1 or
			RCHCC077 = 1 )
	THEN 'Respiratory'  
	WHEN ( CHCC187 = 1 or
	        CHCC189 = 1 or
			CHCC191 = 1 or
			CHCC192 = 1 or
			CHCC193 = 1 or
			CHCC195 = 1 or
			RCHCC051 = 1 or
			CHCC201 = 1 )
    THEN 'Neurologic Conditions'  
	WHEN ( CHCC062 = 1 or
	        CHCC063 = 1 or
			CHCC066 = 1 or
			CHCC068 = 1 or
			CHCC073 = 1 or
			CHCC076 = 1 or
			CHCC080 = 1 or
			CHCC081 = 1 or
			RCHCC109 = 1 )
	THEN 'Gastrointestinal' 
	WHEN ( CHCC082 = 1 or
	        CHCC085 = 1 or
			CHCC089 = 1 or
			CHCC090 = 1 or
			CHCC091 = 1 or
			CHCC092 = 1 or
			CHCC098 = 1 or
			CHCC099 = 1 or
			CHCC107 = 1 or
			CHCC108 = 1 or
			CHCC117 = 1 or
			CHCC119 = 1 or
			CHCC120 = 1 or
			CHCC121	= 1 or
			CHCC128 = 1 )
    THEN 'Musculoskeletal'  
	WHEN ( CHCC003 = 1 or
		    CHCC006 = 1 or
			CHCC046 = 1 or
            CHCC048 = 1 or 
			CHCC051 = 1 or 
			CHCC053 = 1 or 
            CHCC327 = 1 or 
            CHCC369 = 1 or 
			CHCC373 = 1 or 
			CHCC386 = 1 or
            CHCC008 = 1 or
		    CHCC037 = 1 or
			CHCC049 = 1 or
            CHCC052 = 1 or 
			CHCC056 = 1 or 
			CHCC067 = 1 or 
            CHCC069 = 1 or 
            CHCC078 = 1 or 
            CHCC086 = 1 or 
            CHCC103 = 1 or 
            CHCC111 = 1 or 
            CHCC112 = 1 or 
			CHCC113 = 1 or 
			CHCC115 = 1 or 
			CHCC123 = 1 or 
			CHCC132 = 1 or 
			CHCC154 = 1 or 
			CHCC155 = 1 or 
			CHCC169 = 1 or 
			CHCC170 = 1 or 
			CHCC171 = 1 or 
			CHCC172 = 1 or 
			RCHCC046 = 1 or 
            RCHCC047 = 1 or 
			CHCC202 = 1 or
			CHCC203 = 1 or
            CHCC232 = 1 or 
			RCHCC064 = 1 or 
			CHCC272 = 1 or 
            CHCC273 = 1 or 
			CHCC274 = 1 or 
			CHCC276 = 1 or 
			CHCC277 = 1 or 
			CHCC278 = 1 or 
			CHCC279 = 1 or 
			CHCC282 = 1 or 
			CHCC283 = 1 or 
            CHCC284 = 1 or 
			CHCC297 = 1 or 
			CHCC298 = 1 or 
			CHCC299 = 1 or 
			CHCC302 = 1 or 
			CHCC304 = 1 or 
			CHCC306 = 1 or 
            CHCC307 = 1 or 
			CHCC309 = 1 or 
			CHCC313 = 1 or 
			CHCC328 = 1 or 
			CHCC330 = 1 or 
			CHCC343 = 1 or 
			CHCC345 = 1 or 
			CHCC346 = 1 or 
			CHCC370 = 1 or 
			CHCC375 = 1 )
    THEN 'Other Chronic Conditions' 
	WHEN ( CHCC143 = 1 or
	        CHCC146 = 1 or
			CHCC147 = 1 or
			CHCC149 = 1 or
			CHCC150 = 1 or
            CHCC151 = 1 or
			CHCC152 = 1 or
			CHCC153 = 1 or
			CHCC156 = 1 or
			CHCC157 = 1 or
			CHCC158 = 1 or
			CHCC159 = 1 or
			CHCC161 = 1 or
			CHCC162 = 1 or
			CHCC163 = 1 or
			CHCC164 = 1 or
			CHCC165 = 1 or
			CHCC166 = 1 or
			CHCC167 = 1 or
			CHCC168 = 1 )
	THEN 'Behavioral Health'   
	WHEN ( ACHCC024 = 1 )
    THEN 'Obstetric'  
	WHEN ( ACHCC001 = 1 or
	 		ACHCC002 = 1 or
	        ACHCC003 = 1 or
			ACHCC004 = 1 or
			ACHCC005 = 1 or
			ACHCC006 = 1 or
			ACHCC007 = 1 or
			ACHCC008 = 1 or
			ACHCC009 = 1 or
			ACHCC010 = 1 or
			ACHCC011 = 1 or
			ACHCC012 = 1 or
			ACHCC013 = 1 or
			ACHCC014 = 1 or
			ACHCC016 = 1 or
			ACHCC017 = 1 or
			ACHCC018 = 1 or
			ACHCC019 = 1 or
			ACHCC020 = 1 or
			ACHCC021 = 1 or
			ACHCC022 = 1 or
			ACHCC023 = 1 or
			ACHCC025 = 1 or
			ACHCC026 = 1 or
			ACHCC027 = 1 or
			ACHCC029 = 1 or
			(ACHCC030 = 1 and (CHCC383 != 1 and CHCC384 !=1 ) ) or 
            ACHCC031 = 1 )
	THEN 'Acute Conditions'  
	WHEN ( CHCC383 = 1 or
	        CHCC384 = 1 or
            CHCC359 = 1 )
	THEN 'Screening'  
	WHEN (  ACHCC001!=1 and ACHCC002!=1 and ACHCC003!=1 and ACHCC004!=1 and 
	         ACHCC005!=1 and ACHCC006!=1 and ACHCC007!=1 and ACHCC008!=1 and
		     ACHCC009!=1 and ACHCC010!=1 and ACHCC011!=1 and ACHCC012!=1 and
             ACHCC013!=1 and ACHCC014!=1 and ACHCC015!=1 and ACHCC016!=1 and  
             ACHCC017!=1 and ACHCC018!=1 and ACHCC019!=1 and ACHCC020!=1 and
             ACHCC021!=1 and ACHCC022!=1 and ACHCC023!=1 and ACHCC024!=1 and
             ACHCC025!=1 and ACHCC026!=1 and ACHCC027!=1 and ACHCC028!=1 and
             ACHCC029!=1 and ACHCC030!=1 and ACHCC031!=1 )
    THEN 'No Claims/Not Groupable'  
	ELSE 'Other Conditions' 
	END 
;
------------------------------------------------------------
UPDATE $SDM_GROUPER_DXCG_MEDICARE_OUTPUT_STAGE3_$ROMO_$YYYYMM 
SET CAT =
CASE 
	WHEN ( ACHCC028 = 1 and CHCC359 !=1 )
    THEN 'High-Risk High-Cost'  
	WHEN ( ACHCC015 = 1 )
    THEN 'High-Risk High-Cost'  
	WHEN ( RCHCC096 = 1 or
             CHCC184 = 1 or
	         CHCC185 = 1 or
	         CHCC332 = 1 or
		     CHCC333 = 1 or
		     CHCC335 = 1 or
			 CHCC336 = 1 or
		     CHCC337 = 1 )
	THEN 'High-Risk High-Cost'  
    WHEN ( RCHCC014 = 1 or
		     RCHCC019 = 1 or
             CHCC213 = 1 or
			 CHCC214 = 1 or
			 CHCC215 = 1 or
             RCHCC072 = 1 or
             CHCC289 = 1 or
		     CHCC290 = 1 or
             RCHCC108 = 1 or
		     CHCC374 = 1 or
             CHCC380 = 1 )
    THEN 'High-Risk High-Cost'  
	WHEN ( RCHCC086 = 1 or 
            RCHCC087 = 1 )
    THEN 'High-Risk High-Cost'  
	WHEN ( ACHCC002 = 1 or 
            RCHCC004 = 1 or
            CHCC033 = 1 or 
			CHCC135 = 1 or
			CHCC137 = 1 or
			CHCC138 = 1 or
			CHCC140 = 1 or
			RCHCC113 = 1 )
    THEN 'Chronic Conditions'  
	WHEN ( ACHCC017 = 1 and CHCC241 != 1)
	THEN 'Chronic Conditions'  
	WHEN ( RCHCC058 = 1 or
            CHCC212 = 1 or
	        CHCC220 = 1 or
			CHCC222 = 1 or
			CHCC223 = 1 or
			CHCC224 = 1 or
			CHCC226 = 1 or
			RCHCC061 = 1 or
			RCHCC062 = 1 or
            RCHCC069 = 1 or
			CHCC250 = 1 )
	THEN 'Chronic Conditions'  
	WHEN ( ACHCC004 = 1 or
            CHCC269=1 or
            CHCC270=1 )
	THEN 'Chronic Conditions'  
	WHEN ( CHCC259 = 1 or
	        CHCC260 = 1 or
			CHCC261 = 1 or
			CHCC264 = 1 or
			RCHCC077 = 1 )
	THEN 'Chronic Conditions'  
	WHEN ( CHCC187 = 1 or
	        CHCC189 = 1 or
			CHCC191 = 1 or
			CHCC192 = 1 or
			CHCC193 = 1 or
			CHCC195 = 1 or
			RCHCC051 = 1 or
			CHCC201 = 1 )
    THEN 'Chronic Conditions'  
	WHEN ( CHCC062 = 1 or
	        CHCC063 = 1 or
			CHCC066 = 1 or
			CHCC068 = 1 or
			CHCC073 = 1 or
			CHCC076 = 1 or
			CHCC080 = 1 or
			CHCC081 = 1 or
			RCHCC109 = 1 )
	THEN 'Chronic Conditions'  
	WHEN ( CHCC082 = 1 or
	        CHCC085 = 1 or
			CHCC089 = 1 or
			CHCC090 = 1 or
			CHCC091 = 1 or
			CHCC092 = 1 or
			CHCC098 = 1 or
			CHCC099 = 1 or
			CHCC107 = 1 or
			CHCC108 = 1 or
			CHCC117 = 1 or
			CHCC119 = 1 or
			CHCC120 = 1 or
			CHCC121	= 1 or
			CHCC128 = 1 )
    THEN 'Chronic Conditions'  
	WHEN ( CHCC003 = 1 or
		    CHCC006 = 1 or
			CHCC046 = 1 or
            CHCC048 = 1 or 
			CHCC051 = 1 or 
			CHCC053 = 1 or 
            CHCC327 = 1 or 
            CHCC369 = 1 or 
			CHCC373 = 1 or 
			CHCC386 = 1 or
            CHCC008 = 1 or
		    CHCC037 = 1 or
			CHCC049 = 1 or
            CHCC052 = 1 or 
			CHCC056 = 1 or 
			CHCC067 = 1 or 
            CHCC069 = 1 or 
            CHCC078 = 1 or 
            CHCC086 = 1 or 
            CHCC103 = 1 or 
            CHCC111 = 1 or 
            CHCC112 = 1 or 
			CHCC113 = 1 or 
			CHCC115 = 1 or 
			CHCC123 = 1 or 
			CHCC132 = 1 or 
			CHCC154 = 1 or 
			CHCC155 = 1 or 
			CHCC169 = 1 or 
			CHCC170 = 1 or 
			CHCC171 = 1 or 
			CHCC172 = 1 or 
			RCHCC046 = 1 or 
            RCHCC047 = 1 or 
			CHCC202 = 1 or
			CHCC203 = 1 or
            CHCC232 = 1 or 
			RCHCC064 = 1 or 
			CHCC272 = 1 or 
            CHCC273 = 1 or 
			CHCC274 = 1 or 
			CHCC276 = 1 or 
			CHCC277 = 1 or 
			CHCC278 = 1 or 
			CHCC279 = 1 or 
			CHCC282 = 1 or 
			CHCC283 = 1 or 
            CHCC284 = 1 or 
			CHCC297 = 1 or 
			CHCC298 = 1 or 
			CHCC299 = 1 or 
			CHCC302 = 1 or 
			CHCC304 = 1 or 
			CHCC306 = 1 or 
            CHCC307 = 1 or 
			CHCC309 = 1 or 
			CHCC313 = 1 or 
			CHCC328 = 1 or 
			CHCC330 = 1 or 
			CHCC343 = 1 or 
			CHCC345 = 1 or 
			CHCC346 = 1 or 
			CHCC370 = 1 or 
			CHCC375 = 1 )
    THEN 'Chronic Conditions'  
	WHEN ( CHCC143 = 1 or
	        CHCC146 = 1 or
			CHCC147 = 1 or
			CHCC149 = 1 or
			CHCC150 = 1 or
            CHCC151 = 1 or
			CHCC152 = 1 or
			CHCC153 = 1 or
			CHCC156 = 1 or
			CHCC157 = 1 or
			CHCC158 = 1 or
			CHCC159 = 1 or
			CHCC161 = 1 or
			CHCC162 = 1 or
			CHCC163 = 1 or
			CHCC164 = 1 or
			CHCC165 = 1 or
			CHCC166 = 1 or
			CHCC167 = 1 or
			CHCC168 = 1 )
	THEN 'Chronic Conditions'  
	WHEN ( ACHCC024 = 1 )
    THEN 'Healthy Member'  
	WHEN ( ACHCC001 = 1 or
	 		ACHCC002 = 1 or
	        ACHCC003 = 1 or
			ACHCC004 = 1 or
			ACHCC005 = 1 or
			ACHCC006 = 1 or
			ACHCC007 = 1 or
			ACHCC008 = 1 or
			ACHCC009 = 1 or
			ACHCC010 = 1 or
			ACHCC011 = 1 or
			ACHCC012 = 1 or
			ACHCC013 = 1 or
			ACHCC014 = 1 or
			ACHCC016 = 1 or
			ACHCC017 = 1 or
			ACHCC018 = 1 or
			ACHCC019 = 1 or
			ACHCC020 = 1 or
			ACHCC021 = 1 or
			ACHCC022 = 1 or
			ACHCC023 = 1 or
			ACHCC025 = 1 or
			ACHCC026 = 1 or
			ACHCC027 = 1 or
			ACHCC029 = 1 or
			(ACHCC030 = 1 and (CHCC383 != 1 and CHCC384 !=1 ) ) or 
            ACHCC031 = 1 )
	THEN 'Healthy Member'  
	WHEN ( CHCC383 = 1 or
	        CHCC384 = 1 or
            CHCC359 = 1 )
	THEN 'Healthy Member'  
	WHEN (  ACHCC001!=1 and ACHCC002!=1 and ACHCC003!=1 and ACHCC004!=1 and 
	         ACHCC005!=1 and ACHCC006!=1 and ACHCC007!=1 and ACHCC008!=1 and
		     ACHCC009!=1 and ACHCC010!=1 and ACHCC011!=1 and ACHCC012!=1 and
             ACHCC013!=1 and ACHCC014!=1 and ACHCC015!=1 and ACHCC016!=1 and  
             ACHCC017!=1 and ACHCC018!=1 and ACHCC019!=1 and ACHCC020!=1 and
             ACHCC021!=1 and ACHCC022!=1 and ACHCC023!=1 and ACHCC024!=1 and
             ACHCC025!=1 and ACHCC026!=1 and ACHCC027!=1 and ACHCC028!=1 and
             ACHCC029!=1 and ACHCC030!=1 and ACHCC031!=1 )
    THEN 'Healthy Member'  
	ELSE 'Healthy Member' 
	END

;

CREATE TABLE $SDM_GROUPER_DXCG_MEDICARE_OUTPUT_$ROMO_$YYYYMM
AS 
SELECT 
MEMBER_ID,
DOB,
"AGE",
SEX,
MED_MBR_MONTHS,
EXPEND1,
MCAID,
OREC,
ELIGCAT,
Score_AgeGndY1_Med,
Score_AgeGndY2_Med,
Score_Dxy1,
Score_Dxy2,
Risk_Driver_1,
Risk_Driver_1PCT,
Risk_Driver_2,
Risk_Driver_2PCT,
Risk_Driver_3,
Risk_Driver_3PCT,
Risk_Driver_4,
Risk_Driver_4PCT,
Risk_Driver_5,
Risk_Driver_5PCT,
DCG_Y1,
DCG_Y2,
ADCG_Y1,
ADCG_Y2,
C_GROUP,
CAT
FROM $SDM_GROUPER_DXCG_MEDICARE_OUTPUT_STAGE3_$ROMO_$YYYYMM 
;
