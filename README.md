# ciss_web

#### Brief introduction
ciss_web is a strategy platform supportting 24 hours strategy development for professional investment.

#### First level of software architecture
ciss_exhi:  Script directory connecting html file with exhibition result of various functions from CISS_rc
CISS_rc: Script directory for strategy platform such as applications、database、core engine etc.
ciss_web: a structure of the wensite derived from Django 
static: statical files such as html,js, png files
other files：db.sqlite3, manage.py, models.json, models.py,requirements.txt

#### Steps for developing a simple strategy
1. Developping a test script prefixed with "test_" under directory of "apps\" ;
2. Setting related configuration and parameters at a script prefixed with "config_" under directory of "db\" ;
3. Developping related functions in scripts such as "stockpools.py,indicator.py,func_stra.py,signal.py,algo_opt.py";
4. Generating Simulated portfolio holdings,trades,unit performance, test stratefy both in history and by daily in future.
5. Evaluating portfolio performance and analyzing liquidity risk and extreme risks. 

#### Strategy Analysis Steps

1. Assumption, input data
2. Analysis indicators, strategy models, and signals
3. Transactions, Account Plan Management
4. Portfolio Analysis Management
5. Multi portfolio | Multi asset analysis management
6. Restrictions and regulations


#### Notes on data management

The function of data management is mainly achieved through the "Data Management" directory. According to the type of business, it mainly includes the following types of data: time data, asset data, investment related basic information data, stock pool data, analytical indicator data, equation and strategy data, trading signal data, algorithm and optimization data, trading data, account data, portfolio data, fund data, team data, institutional data, market data, log data, user data In/Out (I/O) data. Under the time data subdirectory, different countries and markets such as China, Hong Kong, and the United States can be treated differently in terms of trading days and times. The multi asset category subdirectory contains data scripts used to retrieve data from information service providers' data interfaces (such as Wind-API, Choice-API), perform preliminary analysis, and save it locally. Other commonly used data service providers include Hang Seng, Qianlong, Hengtai, Chaoyang Yongxu, and various market exchanges. There are still many indicators such as daily transaction amounts and enterprise value multiples that cannot be obtained through data interfaces when obtaining market conditions or financial fundamentals data in Hong Kong and the United States. Therefore, in the current strategy development process, manual review and organization of data obtained through data interfaces are still indispensable.

#### Contribute 
1.  Fork repo
2.  New branch 
3.  Submit codes
4.  New pull Request
 
