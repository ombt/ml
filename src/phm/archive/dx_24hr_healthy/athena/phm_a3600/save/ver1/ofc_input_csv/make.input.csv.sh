# 
# ALGORITHM_NAME,PHM_PATTERNS_SK,PARAMETER_NAME,PARAMETER_VALUE
# CC Cuvette Combined,11113,IHN_LEVEL3_DESC,Cuvette Combined
# CC Cuvette Combined,11113,LOGFIELD24_PCT,10
# CC Cuvette Combined,11113,LOGFIELD24_THRESHOLD,15000
# CC Cuvette Combined,11113,LOGFIELD25_PCT,20
# CC Cuvette Combined,11113,LOGFIELD25_THRESHOLD,20000
# CC Cuvette Combined,11113,THRESHOLDS_COUNT,1
# CC Cuvette Combined,11113,THRESHOLD_DESCRIPTION,Cuvette Combined
# 
# PHM_THRESHOLDS_SK,THRESHOLD_NUMBER,THRESHOLD_NUMBER_UNIT,THRESHOLD_NUMBER_DESC,PHM_PATTERNS_SK,THRESHOLDS_SK_VAL,PATTERN_DESCRIPTION,THRESHOLD_ALERT,ALGORITHM_TYPE,THRESHOLD_DATA_DAYS,MODULE_TYPE
# 
(
cat <<EOF
PHM_THRESHOLDS_SK,THRESHOLD_NUMBER,THRESHOLD_NUMBER_UNIT,THRESHOLD_NUMBER_DESC,PHM_PATTERNS_SK,THRESHOLDS_SK_VAL,PATTERN_DESCRIPTION,THRESHOLD_ALERT,ALGORITHM_TYPE,THRESHOLD_DATA_DAYS,MODULE_TYPE
81,3,2,"3/day for 2 consecutive days",81,82,"0665","Low Temperature CM 0665","ERROR_COUNT",2,"CM"
82,1,2,"1/day for 2 consecutive days",82,85,"909B","Heater Error SM 909B","ERROR_COUNT",2,"SM"
83,10,2,"10/day for 2 consecutive days",83,81,"04E2","Carrier Runaway From Load Gate IOM 04E2","ERROR_COUNT",2,"IOM"
84,2,2,"2/day for 2 consecutive days",84,86,"5012","Carrier At STAT Input Gate in Incorrect Status ISR 5012","ERROR_COUNT",2,"ISR"
85,2,2,"2/day for 2 consecutive days",85,89,"501E","Carrier Not Arrived at Barcode Reader Gate from Prepare Gate ISR 501E","ERROR_COUNT",2,"ISR"
86,2,2,"2/day for 2 consecutive days",86,92,"5020","Carrier Not Arrived at Barcode Reader Gate from Priority Input Gate ISR 5020","ERROR_COUNT",2,"ISR"
87,2,2,"2/day for 2 consecutive days",87,95,"501F","Carrier Not Arrived at Barcode Reader Gate from Return Gate ISR 501F","ERROR_COUNT",2,"ISR"
88,10,2,"10/day for 2 consecutive days",88,83,"04F8","Carrier Runaway From Unload Gate IOM 04F8","ERROR_COUNT",2,"IOM"
89,2,2,"2/day for 2 consecutive days",89,102,"5013","Carrier Not Arrived at Output Gate from Return Gate ISR 5013","ERROR_COUNT",2,"ISR"
90,1,2,"1/day for 2 consecutive days",90,87,"909C","Heater Temperature Limit Exceeded SM 909C","ERROR_COUNT",2,"SM"
91,2,2,"2/day for 2 consecutive days",91,109,"5014","Carrier Not Arrived at Output Gate from STAT Input Gate ISR 5014","ERROR_COUNT",2,"ISR"
92,10,2,"10/day for 2 consecutive days",92,84,"06FD","Carrier Runaway From Load Gate CM 06FD","ERROR_COUNT",2,"CM"
93,5,2,"5/day for 2 consecutive days",93,262,"0620","Pass Error - Divert Gate CM 0620","ERROR_COUNT",2,"CM"
94,2,2,"2/day for 2 consecutive days",94,113,"5022","Carrier Not Released on Track from Return Gate ISR 5022","ERROR_COUNT",2,"ISR"
95,1,2,"1/day for 2 consecutive days",95,88,"90A6","Temperature Heater Sensor Error SM 90A6","ERROR_COUNT",2,"SM"
96,5,2,"5/day for 2 consecutive days",96,241,"0621","Pass Error - Unload Gate CM 0621","ERROR_COUNT",2,"CM"
97,4,2,"4/day for 2 consecutive days",97,197,"06BB","Axis Z Down Failure CM 06BB","ERROR_COUNT",2,"CM"
98,10,2,"10/day for 2 consecutive days",98,91,"0B02","Carrier Runaway - Load Gate RIM 0B02","ERROR_COUNT",2,"RIM"
99,2,2,"5/day for 2 consecutive days",99,115,"5023","Carrier Not Released on Track from STAT Input Gate ISR 5023","ERROR_COUNT",2,"ISR"
100,5,2,"2/day for 2 consecutive days",100,94,"0F0A","High Temperature SRM 0F0A","ERROR_COUNT",2,"SRM"
101,5,2,"5/day for 2 consecutive days",101,243,"0622","Pass Error - Load Gate CM 0622","ERROR_COUNT",2,"CM"
102,5,2,"5/day for 2 consecutive days",102,117,"5003","Carrier Run Away from Barcode Reader Gate ISR 5003","ERROR_COUNT",2,"ISR"
103,4,2,"4/day for 2 consecutive days",103,199,"0B9E","Axis Z Down Failure RIM 0B9E","ERROR_COUNT",2,"RIM"
104,10,2,"10/day for 2 consecutive days",104,93,"0BA6","Carrier Runaway - Load Gate RIM 0BA6","ERROR_COUNT",2,"RIM"
105,1.5,2,"1 SD above 30 day mean for 2 consecutive days (high volume)",105,96,"0405","Unreadable Sample ID or Unreadable Rack ID IOM 0405_1","SD_HIGH_VOLUME",30,"IOM"
106,5,2,"5/day for 2 consecutive days",106,244,"0420","Pass Error - Full Carrier Diver Gate IOM 0420","ERROR_COUNT",2,"IOM"
107,5,2,"5/day for 2 consecutive days",107,118,"5001","Carrier Run Away from Divert Gate ISR 5001","ERROR_COUNT",2,"ISR"
108,2,2,"2/day for 2 consecutive days",108,119,"0EB6","Gripper Open Fault During Sample Tube Picking SRM 0EB6","ERROR_COUNT",2,"SRM"
109,4,2,"2/day for 2 consecutive days",109,202,"0D9A","Axis Z Down Failure ROM 0D9A","ERROR_COUNT",2,"ROM"
110,1.5,2,"1SD above 30 day mean for 2 consecutive days (low volume)",110,98,"0405","Unreadable Sample ID or Unreadable Rack ID IOM 0405_2","SD_LOW_VOLUME",30,"IOM"
111,5,2,"5/day for 2 consecutive days",111,246,"0421","Pass Error - Unload Gate IOM 0421","ERROR_COUNT",2,"IOM"
112,5,2,"2/day for 2 consecutive days",112,121,"5007","Carrier Run Away from Output Gate ISR 5007","ERROR_COUNT",2,"ISR"
113,4,2,"4/day for 2 consecutive days",113,207,"0EBB","Axis Z Down Failure SRM 0EBB","ERROR_COUNT",2,"SRM"
114,1.5,2,"1.5%/day for 2 consecutive days",114,99,"E044","Unreadable Sample ID AQM E044","PERCENTAGE",2,"AQM"
115,5,2,"5/day for 2 consecutive days",115,247,"0422","Pass Error - Load Gate IOM 0422","ERROR_COUNT",2,"IOM"
116,2,2,"2/day for 2 consecutive days",116,123,"04B8","Gripper Open Fault During Sample Tube Placing IOM 04B8","ERROR_COUNT",2,"IOM"
117,5,2,"5/day for 2 consecutive days",117,125,"5002","Carrier Run Away from Prepare Gate ISR 5002","ERROR_COUNT",2,"ISM"
118,10,2,"10/day for 2 consecutive days",118,97,"0BFF","Carrier Runaway At Load Gate During Identification RIM 0BFF","ERROR_COUNT",2,"RIM"
119,4,2,"4/day for 2 consecutive days",119,208,"04BA","Axis Z Up Failure IOM 04BA","ERROR_COUNT",2,"IOM"
120,5,2,"5/day for 2 consecutive days",120,248,"0423","Pass Error - Barcode Reader Gate IOM 0423","ERROR_COUNT",2,"IOM"
121,5,2,"5/day for 2 consecutive days",121,127,"5009","Carrier Run Away from Priority Input Gate ISR 5009","ERROR_COUNT",2,"ISR"
122,2,2,"2/day for 2 consecutive days",122,126,"0EB8","Gripper Open Fault During Sample Tube Placing SRM 0EB8","ERROR_COUNT",2,"SRM"
123,5,2,"5/day for 2 consecutive days",123,250,"0424","Pass Error - Empty Carrier Divert Gate IOM 0424","ERROR_COUNT",2,"IOM"
124,4,2,"4/day for 2 consecutive days",124,211,"06BA","Axis Z Up Failure CM 06BA","ERROR_COUNT",2,"CM"
125,5,2,"5/day for 2 consecutive days",125,130,"5006","Carrier Run Away from Return Gate ISR 5006","ERROR_COUNT",2,"ISR"
126,1.5,2,"1.5%/day for 2 consecutive days",126,100,"0605","Unreadable Sample ID CM 0605","PERCENTAGE",2,"CM"
127,5,2,"5/day for 2 consecutive days",127,252,"0425","Pass Error - Empty Carrier Buffer Gate IOM 0425","ERROR_COUNT",2,"IOM"
128,1,1,"1/day",128,131,"5005","Carrier Run Away from Routine Gate ISR 5005","ERROR_COUNT",1,"ISR"
129,4,2,"4/day for 2 consecutive days",129,213,"0B9D","Axis Z Up Failure RIM 0B9D","ERROR_COUNT",2,"RIM"
130,2,2,"2%/day for 2 consecutive days",130,103,"5015","Unreadable Sample ID ISR 5015","PERCENTAGE",2,"ISR"
131,1,1,"1/day",131,255,"5047","Waiting for Passthrough ACK at Divert Gate ISR 5047","ERROR_COUNT",1,"ISR"
132,2,2,"2/day for 2 consecutive days",132,133,"0DA1","Gripper Open Fault During Sample Tube Unloading ROM 0DA1","ERROR_COUNT",2,"ROM"
133,10,2,"10/day for 2 consecutive days",133,101,"0D1B","Carrier Runaway - Divert Gate ROM 0D1B","ERROR_COUNT",2,"ROM"
134,5,2,"5/day for 2 consecutive days",134,134,"5008","Carrier Run Away from Routine Input Gate ISR 5008","ERROR_COUNT",2,"ISR"
135,5,2,"5/day for 2 consecutive days",135,257,"0B10","Pass Error - Divert Gate RIM 0B10","ERROR_COUNT",2,"RIM"
136,4,2,"4/day for 2 consecutive days",136,216,"0D99","Axis Z Up Failure ROM 0D99","ERROR_COUNT",2,"ROM"
137,2,2,"2/day for 2 consecutive days",137,136,"06D4","Sample Tube Lost CM 06D4","ERROR_COUNT",2,"CM"
138,5,2,"5%/day for 2 consecutive days",138,104,"0E05","Unreadable Sample ID SRM 0E05","PERCENTAGE",2,"SRM"
139,10,2,"10/day for 2 consecutive days",139,105,"0D1C","Carrier Runaway - Unload Gate ROM 0D1C","ERROR_COUNT",2,"SRM"
140,1,1,"1/day",140,137,"5004","Carrier Run Away from STAT Gate ISR 5004","ERROR_COUNT",1,"ISR"
141,5,2,"5/day for 2 consecutive days",141,259,"0B11","Pass Error - Load Gate RIM 0B11","ERROR_COUNT",2,"RIM"
142,4,2,"4/day for 2 consecutive days",142,219,"0EBA","Axis Z Up Failure SRM 0EBA","ERROR_COUNT",2,"SRM"
143,1,2,"1/day for 2 consecutive days",143,106,"SC05F","Too Many Sample ID Mismatches ALL SC05F","ERROR_COUNT",2,"%"
144,2,2,"2/day for 2 consecutive days",144,139,"0ED4","Sample Tube Lost SRM 0ED4","ERROR_COUNT",2,"SRM"
145,1,2,"1/day for 2 consecutive days",145,143,"0F82","Rack In Failure SRM 0F82","ERROR_COUNT",2,"SRM"
146,5,2,"5/day for 2 consecutive days",146,261,"0B12","Pass Error - Barcode Reader Gate RIM 0B12","ERROR_COUNT",2,"RIM"
147,10,2,"10/day for 2 consecutive days",147,107,"0DA5","Carrier Runaway - Unload Gate ROM 0DA5","ERROR_COUNT",2,"ROM"
148,2,2,"2/day for 2 consecutive days",148,222,"0492","Moving Fault - Axis X IOM 0492","ERROR_COUNT",2,"IOM"
149,2,2,"2/day for 2 consecutive days",149,142,"06B6","Tube Gripper Open Fault During Sample Tube Picking CM 06B6","ERROR_COUNT",2,"CM"
150,5,2,"5/day for 2 consecutive days",150,260,"0D16","Pass Error - Divert Gate ROM 0D16","ERROR_COUNT",2,"ROM"
151,1,2,"1/day for 2 consecutive days",151,148,"0F83","Rack Out Failure SRM 0F83","ERROR_COUNT",2,"SRM"
152,1,1,"1/day",152,108,"503B","Sample Presentation Error At Routine Gate ISR 503B","ERROR_COUNT",1,"ISR"
153,10,2,"10/day for 2 consecutive days",153,112,"0E76","Sample Load Interrupted due to Carrier Runaway SRM 0E76","ERROR_COUNT",2,"SRM"
154,5,2,"5/day for 2 consecutive days",154,256,"0D17","Pass Error - Load Gate ROM 0D17","ERROR_COUNT",2,"ROM"
155,2,2,"2/day for 2 consecutive days",155,146,"06B8","Tube Gripper Open Fault During Sample Tube Placing CM 06B8","ERROR_COUNT",2,"CM"
156,2,2,"2/day for 2 consecutive days",156,225,"0692","Moving Fault - Axis X CM 0692","ERROR_COUNT",2,"CM"
157,2,2,"2/day for 2 consecutive days",157,152,"0ED6","Tube Disposal Failure SRM 0ED6","ERROR_COUNT",2,"SRM"
158,1,1,"1/day",158,110,"503A","Sample Presentation Error at STAT Gate ISR 503A","ERROR_COUNT",1,"ISR"
159,2,2,"2/day for 2 consecutive days",159,150,"9011","Head Down Failure SM 9011","ERROR_COUNT",2,"SM"
160,1,1,"1/day",160,231,"0D22","Interface Module Is Waiting For Sample Carrier Passing Notification Acknowledge ROM 0D22","ERROR_COUNT",1,"ROM"
161,2,2,"2/day for 2 consecutive days",161,90,"0FFA","Tube Drop Failure SRM 0FFA","ERROR_COUNT",2,"SRM"
162,10,2,"10/day for 2 consecutive days",162,116,"0ED7","Carrier Runaway from Unload Gate SRM 0ED7","ERROR_COUNT",2,"SRM"
163,2,2,"2/day for 2 consecutive days",163,221,"0E92","Moving Fault - Axis X SRM 0E92","ERROR_COUNT",2,"SRM"
164,1,1,"1/day",164,111,"508F","Sample Queue Error At Routine Gate ISR 508F","ERROR_COUNT",1,"ISR"
165,5,2,"5/day for 2 consecutive days",165,254,"0E20","Pass Error - Divert Gate SRM 0E20","ERROR_COUNT",2,"ISR"
166,1,2,"1/day for 2 consecutive days",166,153,"9090","Head Up Failure SM 9090","ERROR_COUNT",2,"SM"
167,2,2,"2/day for 2 consecutive days",167,220,"0491","Moving Fault - Axis Y IOM 0491","ERROR_COUNT",2,"IOM"
168,10,2,"10/day for 2 consecutive days",168,120,"0FF8","Carrier Runaway From Load Gate SRM 0FF8","ERROR_COUNT",2,"SRM"
169,1,1,"1/day",169,114,"508A","Sample Queue Error at STAT Gate ISR 508A","ERROR_COUNT",1,"ISR"
170,10,2,"5/day for 2 consecutive days",170,253,"0E21","Pass Error - Unload Gate SRM 0E21","ERROR_COUNT",2,"SRM"
171,1,2,"1/day for 2 consecutive days",171,155,"908C","Head Rotation To Foil Pick Position Error SM 908C","ERROR_COUNT",2,"SM"
172,2,2,"2/day for 2 consecutive days",172,218,"0691","Moving Fault - Axis Y CM 0691","ERROR_COUNT",2,"CM"
173,5,2,"5/day for 2 consecutive days",173,249,"0E22","Pass Error - Load Gate SRM 0E22","ERROR_COUNT",2,"SRM"
174,10,2,"10/day for 2 consecutive days",174,122,"2AFF","Carrier Runaway At Load Gate During Identification IOM 2AFF","ERROR_COUNT",2,"IOM"
175,1,1,"1/day",175,163,"5089","Sample Presentation Error and Level Sense Error ISR 5089","ERROR_COUNT",1,"ISR"
176,1,1,"1/day for 2 consecutive days",176,157,"908B","Head Rotation To Seal Position Error SM 908B","ERROR_COUNT",1,"SM"
177,5,2,"5/day for 2 consecutive days",177,245,"0E23","Pass Error - Barcode Reader Gate SRM 0E23","ERROR_COUNT",2,"SRM"
178,2,2,"2/day for 2 consecutive days",178,215,"0B97","Moving Fault - Axis Y RIM 0B97","ERROR_COUNT",2,"RIM"
179,1,1,"1/day",179,185,"7087","Sample Presentation Error and Level Sense Error C16 7087","ERROR_COUNT",1,"C16"
180,1,1,"1/day",180,124,"0453","Waiting For Run Away Ack Notification - Barcode Reader Gate IOM 0453","ERROR_COUNT",1,"IOM"
181,10,2,"10/day for 2 consecutive days",181,242,"0476","Sample Load Interrupted due to Carrier Runaway IOM 0476","ERROR_COUNT",2,"IOM"
182,2,2,"2/day for 2 consecutive days",182,160,"9097","Foil Lost SM 9097","ERROR_COUNT",2,"SM"
183,2,2,"2/day for 2 consecutive days",183,212,"0D96","Moving Fault - Axis Y ROM 0D96","ERROR_COUNT",2,"ROM"
184,1,1,"1/day",184,177,"7086","Sample Queue Error C16 7086","ERROR_COUNT",1,"C16"
185,2,2,"2/day for 2 consecutive days",185,210,"0E91","Moving Fault - Axis Y SRM 0E91","ERROR_COUNT",2,"SRM"
186,4,2,"4/day for 2 consecutive days",186,161,"1006","Head Down Failure DCM 1006","ERROR_COUNT",2,"DCM"
187,1,1,"1/day",187,172,"7088","Sample Queue Error On Carrier Not In List C16 7088","ERROR_COUNT",1,"C16"
188,1,2,"1/day for 2 consecutive days",188,159,"0F8C","Automated Refrigerator Barrier Failure SRM 0F8C","ERROR_COUNT",2,"SRM"
189,2,2,"2/day for 2 consecutive days",189,205,"0493","Moving Fault - Axes X and Y IOM 0493","ERROR_COUNT",2,"IOM"
190,1,1,"1/day",190,187,"5090","Sample Queue Error On Carrier Not In List At Routine Gate ISR 5090","ERROR_COUNT",1,"ISR"
191,1,2,"1/day for 2 consecutive days",191,165,"108A","Head Up Failure DCM 108A","ERROR_COUNT",2,"DCM"
192,1,2,"1/day for 2 consecutive days",192,164,"0F8D","I/O Barrier Failure SRM 0F8D","ERROR_COUNT",2,"SRM"
193,2,2,"2/day for 2 consecutive days",193,204,"0693","Moving Fault - Axes X and Y CM 0693","ERROR_COUNT",2,"CM"
194,1,2,"1/day for 2 consecutive days",194,169,"0FE7","Internal Barrier Sensor Engaged SRM 0FE7","ERROR_COUNT",2,"SRM"
195,2,2,"2/day for 2 consecutive days",195,201,"0E93","Moving Fault - Axes X And Y SRM 0E93","ERROR_COUNT",2,"SRM"
196,1,2,"1/day for 2 consecutive days",196,168,"1082","Head Rotation To Decap Position Error DCM 1082","ERROR_COUNT",2,"DCM"
197,1,1,"1/day",197,128,"0653","Waiting For Run Away Ack Notification - Load Gate CM 0653","ERROR_COUNT",1,"CM"
198,2,2,"2/day for 2 consecutive days",198,233,"504D","Automation Carrier with Routing Error at Output Gate ISR 504D","ERROR_COUNT",2,"ISR"
199,1,2,"1/day for 2 consecutive days",199,175,"049F","Axes X And Y Moving Fault Recovery has Failed IOM 049F","ERROR_COUNT",2,"IOM"
200,2,2,"2/day for 2 consecutive days",200,200,"E0EF","Pipettor Moving Fault Error AQM E0EF","ERROR_COUNT",2,"AQM"
201,1,2,"1/day for 2 consecutive days",201,170,"1083","Head Rotation To Waste Position Error DCM 1083","ERROR_COUNT",2,"DCM"
202,10,2,"10/day for 2 consecutive days",202,129,"0675","Empty Carrier Run Away CM 0675","ERROR_COUNT",2,"CM"
203,1,2,"1/day for 2 consecutive days",203,178,"069F","Axes X And Y Moving Fault Recovery has Failed CM 069F","ERROR_COUNT",2,"CM"
204,1,2,"1/day for 2 consecutive days",204,173,"108D","Incorrect Head Rotation DCM 108D","ERROR_COUNT",2,"DCM"
205,2,2,"2/day for 2 consecutive days",205,195,"E0B5","Robotic Arm Moving Fault AQM E0B5","ERROR_COUNT",2,"AQM"
206,1,1,"1/day",206,189,"508B","Sample Queue Error On Carrier Not In List At Stat Gate ISR 508B","ERROR_COUNT",1,"ISR"
207,1,2,"1/day for 2 consecutive days",207,181,"0E9F","Axes X And Y Moving Fault Recovery has Failed SRM 0E9F","ERROR_COUNT",2,"SRM"
208,10,2,"10/day for 2 consecutive days",208,132,"06D5","Empty Carrier Run Away CM 06D5","ERROR_COUNT",2,"CM"
209,3,2,"3/day for 2 consecutive days",209,174,"1087","Cap Drop Failure DCM 1087","ERROR_COUNT",2,"DCM"
210,2,2,"2/day for 2 consecutive days",210,167,"504F","Automation Carrier with Routing Error at Priority Input Gate ISR 504F","ERROR_COUNT",2,"ISR"
211,2,2,"2/day for 2 consecutive days",211,196,"E0B4","Gripper Open Failure AQM E0B4","ERROR_COUNT",2,"AQM"
212,1,2,"1/day for 2 consecutive days",212,183,"0B9C","Axis Moving Fault Recovery Has Failed RIM 0B9C","ERROR_COUNT",2,"RIM"
213,10,2,"10/day for 2 consecutive days",213,135,"0E53","Waiting For Run Away Ack Notification - Barcode Reader Gate SRM 0E53","ERROR_COUNT",2,"SRM"
214,1,2,"1/day for 2 consecutive days",214,176,"108B","Cap Drop Sensor Error DCM 108B","ERROR_COUNT",2,"DCM"
215,2,2,"2/day for 2 consecutive days",215,166,"504C","Automation Carrier with Routing Error at Return Gate ISR 504C","ERROR_COUNT",2,"ISR"
216,2,2,"2/day for 2 consecutive days",216,198,"04C3","Gripper Open Fault IOM 04C3","ERROR_COUNT",2,"IOM"
217,1,2,"1/day for 2 consecutive days",217,184,"0D98","Axis Moving Fault Recovery Has Failed ROM 0D98","ERROR_COUNT",2,"ROM"
218,2,2,"2/day for 2 consecutive days",218,162,"504E","Automation Carrier with Routing Error at Routine Input Gate ISR 504E","ERROR_COUNT",2,"ISR"
219,2,2,"2/day for 2 consecutive days",219,203,"0B8E","Gripper Open Fault RIM 0B8E","ERROR_COUNT",2,"RIM"
220,1,2,"1/day for 2 consecutive days",220,186,"049E","Axis X Moving Fault Recovery has Failed IOM 049E","ERROR_COUNT",2,"IOM"
221,1,2,"1/day for 2 consecutive days",221,179,"1091","Cap Waste Sensor Error DCM 1091","ERROR_COUNT",2,"DCM"
222,10,2,"10/day for 2 consecutive days",222,140,"0ED5","Empty Carrier Run Away SRM 0ED5","ERROR_COUNT",2,"SRM"
223,2,2,"2/day for 2 consecutive days",223,158,"500C","Carrier At Barcode Reader Gate in Incorrect Status ISR 500C","ERROR_COUNT",2,"ISR"
224,2,2,"2/day for 2 consecutive days",224,206,"0D8B","Gripper Open Fault ROM 0D8B","ERROR_COUNT",2,"ROM"
225,5,2,"5/day for 2 consecutive days",225,180,"7020","Pass Error - Divert Gate C16 7020","ERROR_COUNT",2,"C16"
226,1,2,"1/day for 2 consecutive days",226,188,"069E","Axis X Moving Fault Recovery has Failed CM 069E","ERROR_COUNT",2,"CM"
227,2,2,"2/day for 2 consecutive days",227,156,"500A","Carrier At Divert Gate in Incorrect Status ISR 500A","ERROR_COUNT",2,"ISR"
228,10,2,"10/day for 2 consecutive days",228,144,"8018","Carrier Run Away IOM 8018","ERROR_COUNT",2,"IOM"
229,2,2,"2/day for 2 consecutive days",229,209,"0EC3","Gripper Open Fault SRM 0EC3","ERROR_COUNT",2,"SRM"
230,1,1,"1/day",230,182,"7021","Pass Error - Sampling Gate C16 7021","ERROR_COUNT",1,"C16"
231,2,2,"2/day for 2 consecutive days",231,214,"06C3","Tube Gripper Open Fault CM 06C3","ERROR_COUNT",2,"CM"
232,1,2,"1/day for 2 consecutive days",232,147,"1E8B","Conveyor Move Error SRM 1E8B","ERROR_COUNT",2,"SRM"
233,1,2,"1/day for 2 consecutive days",233,190,"0E9E","Axis X Moving Fault Recovery has Failed SRM 0E9E","ERROR_COUNT",2,"SRM"
234,2,2,"2/day for 2 consecutive days",234,217,"04D6","Gripper Initialization Failure IOM 04D6","ERROR_COUNT",2,"IOM"
235,2,2,"2/day for 2 consecutive days",235,138,"5010","Carrier At Output Gate in Incorrect Status ISR 5010","ERROR_COUNT",2,"ISR"
236,1,2,"1/day for 2 consecutive days",236,191,"049D","Axis Y Moving Fault Recovery has Failed IOM 049D","ERROR_COUNT",2,"IOM"
237,2,2,"2/day for 2 consecutive days",237,223,"06D3","Tube Gripper Initialization Failure CM 06D3","ERROR_COUNT",2,"CM"
238,2,2,"2/day for 2 consecutive days",238,230,"500B","Carrier At Prepare Gate in Incorrect Status ISR 500B","ERROR_COUNT",2,"ISR"
239,1,2,"1/day for 2 consecutive days",239,192,"069D","Axis Y Moving Fault Recovery has Failed CM 069D","ERROR_COUNT",2,"CM"
240,2,2,"2/day for 2 consecutive days",240,226,"0ED3","Tube Gripper Initialization Failure SRM 0ED3","ERROR_COUNT",2,"SRM"
241,2,2,"2/day for 2 consecutive days",241,145,"500F","Carrier At Return Gate in Incorrect Status ISR 500F","ERROR_COUNT",2,"ISR"
242,2,2,"2/day for 2 consecutive days",242,149,"500E","Carrier At Routine Gate in Incorrect Status ISR 500E","ERROR_COUNT",2,"ISR"
243,1,2,"1/day for 2 consecutive days",243,193,"0E9D","Axis Y Moving Fault Recovery has Failed SRM 0E9D","ERROR_COUNT",2,"SRM"
244,2,2,"2/day for 2 consecutive days",244,227,"0BA3","Gripper Open Fault During Sample Tube Loading RIM 0BA3","ERROR_COUNT",2,"RIM"
245,2,2,"2/day for 2 consecutive days",245,151,"5011","Carrier At Routine Input Gate in Incorrect Status ISR 5011","ERROR_COUNT",2,"ISR"
246,2,2,"2/day for 2 consecutive days",246,228,"04B6","Gripper Open Fault During Sample Tube Picking IOM 04B6","ERROR_COUNT",2,"IOM"
247,4,2,"4/day for 2 consecutive days",247,194,"04BB","Axis Z Down Failure IOM 04BB","ERROR_COUNT",2,"IOM"
248,2,2,"2/day for 2 consecutive days",248,154,"500D","Carrier At STAT Gate in Incorrect Status ISR 500D","ERROR_COUNT",2,"ISR"
249,2,2,"2/day for 2 consecutive days",249,229,"0BA2","Gripper Open Fault During Sample Tube Picking RIM 0BA2","ERROR_COUNT",2,"RIM"
250,2,2,"2/day for 2 consecutive days",250,232,"0D9E","Gripper Open Fault During Sample Tube Picking ROM 0D9E","ERROR_COUNT",2,"ROM"
EOF
) |
sed 's/"//g' |
gawk '
BEGIN {
    FS = ","
}
NR == 1 {
	phm_thresholds_sk = $1
	threshold_number = $2
	threshold_number_unit = $3
	threshold_number_desc = $4
	phm_patterns_sk = $5
	thresholds_sk_val = $6
	pattern_description = $7
	threshold_alert = $8
	algorithm_type = $9
	threshold_data_days = $10
	module_type = $11
	#
	print "ALGORITHM_NAME,PHM_PATTERNS_SK,PARAMETER_NAME,PARAMETER_VALUE"
	next
}
{
	print "A3600 " $8 "," $1 "," phm_thresholds_sk "," $1
	print "A3600 " $8 "," $1 "," threshold_number "," $2
	print "A3600 " $8 "," $1 "," threshold_number_unit "," $3
	print "A3600 " $8 "," $1 "," threshold_number_desc "," $4
	print "A3600 " $8 "," $1 "," phm_patterns_sk "," $5
	print "A3600 " $8 "," $1 "," thresholds_sk_val "," $6
	print "A3600 " $8 "," $1 "," pattern_description "," $7
	print "A3600 " $8 "," $1 "," threshold_alert "," $8
	print "A3600 " $8 "," $1 "," algorithm_type "," $9
	print "A3600 " $8 "," $1 "," threshold_data_days "," $10
	print "A3600 " $8 "," $1 "," module_type "," $11
	print "A3600 " $8 "," $1 ",IHN_LEVEL3_DESC," $8
	print "A3600 " $8 "," $1 ",THRESHOLD_DESCRIPTION," $8
	next
}
END {
} ' 
#
exit 0
