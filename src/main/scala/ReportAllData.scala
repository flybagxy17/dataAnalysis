/**
  * {"database":"tybdata","es":1549331895000,"id":26374,"isDdl":false,"data":[{"usreId":267,"creatTime":"2018-06-26 16:05:06","regTime":"2018-06-25 16:05:06","isReg":0,"isFirstCharge":1,"firstChargeMoney":100,"isFirstBetToday":0,"betAmount":0,"validBetAmount":0,"netMoney":0,"isFirstChargeToday":1,"Charge":100,"isOutMoneyToday":0,"OutMoney":0,"api_name":"AG","ip":"www.yabo.com","redBet":0,"backBet":0,"preferential":0,"agent":0,"field1":0,"field2":0,"field3":0,"field4":0,"field5":0}]}
  */
case class ReportAllData(
                          database:String,
                          es:String,
                          id:String,
                          isDdl:String,
                          data:ReportData
                        )
