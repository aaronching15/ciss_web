



'''
190124
Get real time bond data from website of China bond 
http://www.chinamoney.com.cn/chinese/mkdatabondcbcbm/

1, 
<table class="san-sheet-alternating">
<thead>
    <tr>
       <td class="cell AC" data-name="abdAssetEncdShrtDesc">债券简称</td>
       <td class="cell AC" data-name="dmiLatestRate">成交净价(元)</td>
       <td class="cell AC" data-name="dmiLatestContraRate">最新收益率(%)</td>
       <td class="cell AC" data-name="bp" data-tag="priceLimit" data-tag-target="bpNum" data-tag-position="right">涨跌(BP)</td>
       <td class="cell AC" data-name="dmiWghtdContraRate">加权收益率(%)</td>
       <td class="cell AC" data-name="dmiTtlTradedAmnt">交易量(亿)</td>
    </tr>
</thead>
<tbody>
    <tr data-row="0"><td class="cell AC" data-name="abdAssetEncdShrtDesc" data-value="18兰花CP001">
    <span class="cell-value"><a href="/chinese/zqjc/?bondDefinedCode=113421525" target="_blank">18兰花CP001</a></span></td>
    <td class="cell AC" data-name="dmiLatestRate" data-value="99.80"><span class="cell-value">99.80</span></td>
    <td class="cell AC" data-name="dmiLatestContraRate" data-value="8.5540"><span class="cell-value">8.5540</span></td>
    <td class="cell AC" data-name="bp" data-value="30.94" data-tag-target="bpNum"><span class="cell-value text-up">30.94</span><span class="san-icon m-l-2 san-icon-up"></span></td>
    <td class="cell AC" data-name="dmiWghtdContraRate" data-value="8.5541"><span class="cell-value">8.5541</span></td>
    <td class="cell AC" data-name="dmiTtlTradedAmnt" data-value="---"><span class="cell-value">---</span></td></tr><tr data-row="1"><td class="cell AC" data-name="abdAssetEncdShrtDesc" data-value="11同煤债02"><span class="cell-value"><a href="/chinese/zqjc/?bondDefinedCode=1000029268" target="_blank">11同煤债02</a></span></td>
    <td class="cell AC" data-name="dmiLatestRate" data-value="103.57"><span class="cell-value">103.57</span></td>
    <td class="cell AC" data-name="dmiLatestContraRate" data-value="5.5000"><span class="cell-value">5.5000</span></td>
    <td class="cell AC" data-name="bp" data-value="10.00" data-tag-target="bpNum"><span class="cell-value text-up">10.00</span><span class="san-icon m-l-2 san-icon-up"></span></td>
    <td class="cell AC" data-name="dmiWghtdContraRate" data-value="5.3974"><span class="cell-value">5.3974</span></td>
    <td class="cell AC" data-name="dmiTtlTradedAmnt" data-value="---"><span class="cell-value">---</span></td></tr><tr data-row="2"><td class="cell AC" data-name="abdAssetEncdShrtDesc" data-value="17国开10"><span class="cell-value"><a href="/chinese/zqjc/?bondDefinedCode=1000095690" target="_blank">17国开10</a></span></td>
    <td class="cell AC" data-name="dmiLatestRate" data-value="101.63"><span class="cell-value">101.63</span></td>
    <td class="cell AC" data-name="dmiLatestContraRate" data-value="3.8035"><span class="cell-value">3.8035</span></td>
    <td class="cell AC" data-name="bp" data-value="0.65" data-tag-target="bpNum"><span class="cell-value text-down">0.65</span><span class="san-icon m-l-2 san-icon-down"></span></td>
    <td class="cell AC" data-name="dmiWghtdContraRate" data-value="3.8068"><span class="cell-value">3.8068</span></td>
    <td class="cell AC" data-name="dmiTtlTradedAmnt" data-value="---"><span class="cell-value">---</span></td></tr><tr data-row="3"><td class="cell AC" data-name="abdAssetEncdShrtDesc" data-value="16附息国债23"><span class="cell-value"><a href="/chinese/zqjc/?bondDefinedCode=1000082184" target="_blank">16附息国债23</a></span></td>
    <td class="cell AC" data-name="dmiLatestRate" data-value="96.18"><span class="cell-value">96.18</span></td>
    <td class="cell AC" data-name="dmiLatestContraRate" data-value="3.2600"><span class="cell-value">3.2600</span></td>
    <td class="cell AC" data-name="bp" data-value="0.50" data-tag-target="bpNum"><span class="cell-value text-down">0.50</span><span class="san-icon m-l-2 san-icon-down"></span></td>
    <td class="cell AC" data-name="dmiWghtdContraRate" data-value="3.2751"><span class="cell-value">3.2751</span></td>
    <td class="cell AC" data-name="dmiTtlTradedAmnt" data-value="---"><span class="cell-value">---</span></td></tr><tr data-row="4"><td class="cell AC" data-name="abdAssetEncdShrtDesc" data-value="14闽漳龙MTN001(5年期)"><span class="cell-value"><a href="/chinese/zqjc/?bondDefinedCode=3883858007" target="_blank">14闽漳龙MTN001(5年期)</a></span></td>
    <td class="cell AC" data-name="dmiLatestRate" data-value="100.57"><span class="cell-value">100.57</span></td>
    <td class="cell AC" data-name="dmiLatestContraRate" data-value="3.5543"><span class="cell-value">3.5543</span></td>
    <td class="cell AC" data-name="bp" data-value="13.40" data-tag-target="bpNum"><span class="cell-value text-down">13.40</span><span class="san-icon m-l-2 san-icon-down"></span></td>
    <td class="cell AC" data-name="dmiWghtdContraRate" data-value="3.5543"><span class="cell-value">3.5543</span></td>
    <td class="cell AC" data-name="dmiTtlTradedAmnt" data-value="---"><span class="cell-value">---</span></td></tr><tr data-row="5"><td class="cell AC" data-name="abdAssetEncdShrtDesc" data-value="18国开14"><span class="cell-value"><a href="/chinese/zqjc/?bondDefinedCode=50668n784k" target="_blank">18国开14</a></span></td>
    <td class="cell AC" data-name="dmiLatestRate" data-value="102.69"><span class="cell-value">102.69</span></td>
    <td class="cell AC" data-name="dmiLatestContraRate" data-value="3.6900"><span class="cell-value">3.6900</span></td>
    <td class="cell AC" data-name="bp" data-value="1.00" data-tag-target="bpNum"><span class="cell-value text-down">1.00</span><span class="san-icon m-l-2 san-icon-down"></span></td>
    <td class="cell AC" data-name="dmiWghtdContraRate" data-value="3.6905"><span class="cell-value">3.6905</span></td>
    <td class="cell AC" data-name="dmiTtlTradedAmnt" data-value="---"><span class="cell-value">---</span></td></tr><tr data-row="6"><td class="cell AC" data-name="abdAssetEncdShrtDesc" data-value="18农发06"><span class="cell-value"><a href="/chinese/zqjc/?bondDefinedCode=3054580406" target="_blank">18农发06</a></span></td>
    <td class="cell AC" data-name="dmiLatestRate" data-value="106.51"><span class="cell-value">106.51</span></td>
    <td class="cell AC" data-name="dmiLatestContraRate" data-value="3.8032"><span class="cell-value">3.8032</span></td>
    <td class="cell AC" data-name="bp" data-value="3.32" data-tag-target="bpNum"><span class="cell-value text-up">3.32</span><span class="san-icon m-l-2 san-icon-up"></span></td>
    <td class="cell AC" data-name="dmiWghtdContraRate" data-value="3.7931"><span class="cell-value">3.7931</span></td>
    <td class="cell AC" data-name="dmiTtlTradedAmnt" data-value="---"><span class="cell-value">---</span></td></tr><tr data-row="7"><td class="cell AC" data-name="abdAssetEncdShrtDesc" data-value="18北方铜业MTN001"><span class="cell-value"><a href="/chinese/zqjc/?bondDefinedCode=1000129913" target="_blank">18北方铜业MTN001</a></span></td>
    <td class="cell AC" data-name="dmiLatestRate" data-value="103.27"><span class="cell-value">103.27</span></td>
    <td class="cell AC" data-name="dmiLatestContraRate" data-value="4.1000"><span class="cell-value">4.1000</span></td>
    <td class="cell AC" data-name="bp" data-value="10.00" data-tag-target="bpNum"><span class="cell-value text-up">10.00</span><span class="san-icon m-l-2 san-icon-up"></span></td>
    <td class="cell AC" data-name="dmiWghtdContraRate" data-value="4.1000"><span class="cell-value">4.1000</span></td>
    <td class="cell AC" data-name="dmiTtlTradedAmnt" data-value="---"><span class="cell-value">---</span></td></tr><tr data-row="8"><td class="cell AC" data-name="abdAssetEncdShrtDesc" data-value="15新华联控MTN001"><span class="cell-value"><a href="/chinese/zqjc/?bondDefinedCode=1000050780" target="_blank">15新华联控MTN001</a></span></td>
    <td class="cell AC" data-name="dmiLatestRate" data-value="75.20"><span class="cell-value">75.20</span></td>
    <td class="cell AC" data-name="dmiLatestContraRate" data-value="38.1700"><span class="cell-value">38.1700</span></td>
    <td class="cell AC" data-name="bp" data-value="3033.36" data-tag-target="bpNum"><span class="cell-value text-up">3033.36</span><span class="san-icon m-l-2 san-icon-up"></span></td>
    <td class="cell AC" data-name="dmiWghtdContraRate" data-value="38.1700"><span class="cell-value">38.1700</span></td>
    <td class="cell AC" data-name="dmiTtlTradedAmnt" data-value="---"><span class="cell-value">---</span></td></tr><tr data-row="9"><td class="cell AC" data-name="abdAssetEncdShrtDesc" data-value="18附息国债11"><span class="cell-value"><a href="/chinese/zqjc/?bondDefinedCode=3074280011" target="_blank">18附息国债11</a></span></td>
    <td class="cell AC" data-name="dmiLatestRate" data-value="104.41"><span class="cell-value">104.41</span></td>
    <td class="cell AC" data-name="dmiLatestContraRate" data-value="3.1400"><span class="cell-value">3.1400</span></td>
    <td class="cell AC" data-name="bp" data-value="0.00" data-tag-target="bpNum"><span class="cell-value text-up">0.00</span><span class="san-icon m-l-2 san-icon-up"></span></td>
    <td class="cell AC" data-name="dmiWghtdContraRate" data-value="3.1402"><span class="cell-value">3.1402</span></td>
    <td class="cell AC" data-name="dmiTtlTradedAmnt" data-value="---"><span class="cell-value">---</span></td></tr><tr data-row="10"><td class="cell AC" data-name="abdAssetEncdShrtDesc" data-value="19东莞农商绿色金融01"><span class="cell-value"><a href="/chinese/zqjc/?bondDefinedCode=62184d25o2" target="_blank">19东莞农商绿色金融01</a></span></td>
    <td class="cell AC" data-name="dmiLatestRate" data-value="100.00"><span class="cell-value">100.00</span></td>
    <td class="cell AC" data-name="dmiLatestContraRate" data-value="3.4999"><span class="cell-value">3.4999</span></td>
    <td class="cell AC" data-name="bp" data-value="0.01" data-tag-target="bpNum"><span class="cell-value text-down">0.01</span><span class="san-icon m-l-2 san-icon-down"></span></td>
    <td class="cell AC" data-name="dmiWghtdContraRate" data-value="3.4999"><span class="cell-value">3.4999</span></td>
    <td class="cell AC" data-name="dmiTtlTradedAmnt" data-value="---"><span class="cell-value">---</span></td></tr><tr data-row="11"><td class="cell AC" data-name="abdAssetEncdShrtDesc" data-value="17附息国债04"><span class="cell-value"><a href="/chinese/zqjc/?bondDefinedCode=1000089448" target="_blank">17附息国债04</a></span></td>
    <td class="cell AC" data-name="dmiLatestRate" data-value="101.95"><span class="cell-value">101.95</span></td>
    <td class="cell AC" data-name="dmiLatestContraRate" data-value="3.1239"><span class="cell-value">3.1239</span></td>
    <td class="cell AC" data-name="bp" data-value="1.39" data-tag-target="bpNum"><span class="cell-value text-up">1.39</span><span class="san-icon m-l-2 san-icon-up"></span></td>
    <td class="cell AC" data-name="dmiWghtdContraRate" data-value="3.1239"><span class="cell-value">3.1239</span></td>
    <td class="cell AC" data-name="dmiTtlTradedAmnt" data-value="---"><span class="cell-value">---</span></td></tr><tr data-row="12"><td class="cell AC" data-name="abdAssetEncdShrtDesc" data-value="19国开05"><span class="cell-value"><a href="/chinese/zqjc/?bondDefinedCode=60624oyrkn" target="_blank">19国开05</a></span></td>
    <td class="cell AC" data-name="dmiLatestRate" data-value="99.19"><span class="cell-value">99.19</span></td>
    <td class="cell AC" data-name="dmiLatestContraRate" data-value="3.5775"><span class="cell-value">3.5775</span></td>
    <td class="cell AC" data-name="bp" data-value="0.75" data-tag-target="bpNum"><span class="cell-value text-down">0.75</span><span class="san-icon m-l-2 san-icon-down"></span></td>
    <td class="cell AC" data-name="dmiWghtdContraRate" data-value="3.5335"><span class="cell-value">3.5335</span></td>
    <td class="cell AC" data-name="dmiTtlTradedAmnt" data-value="---"><span class="cell-value">---</span></td></tr><tr data-row="13"><td class="cell AC" data-name="abdAssetEncdShrtDesc" data-value="16首农MTN001"><span class="cell-value"><a href="/chinese/zqjc/?bondDefinedCode=1000078588" target="_blank">16首农MTN001</a></span></td>
    <td class="cell AC" data-name="dmiLatestRate" data-value="99.81"><span class="cell-value">99.81</span></td>
    <td class="cell AC" data-name="dmiLatestContraRate" data-value="3.4064"><span class="cell-value">3.4064</span></td>
    <td class="cell AC" data-name="bp" data-value="7.41" data-tag-target="bpNum"><span class="cell-value text-down">7.41</span><span class="san-icon m-l-2 san-icon-down"></span></td>
    <td class="cell AC" data-name="dmiWghtdContraRate" data-value="3.4001"><span class="cell-value">3.4001</span></td><td class="cell AC" data-name="dmiTtlTradedAmnt" data-value="---"><span class="cell-value">---</span></td></tr><tr data-row="14"><td class="cell AC" data-name="abdAssetEncdShrtDesc" data-value="16进出02"><span class="cell-value"><a href="/chinese/zqjc/?bondDefinedCode=1000063771" target="_blank">16进出02</a></span></td><td class="cell AC" data-name="dmiLatestRate" data-value="99.97"><span class="cell-value">99.97</span></td><td class="cell AC" data-name="dmiLatestContraRate" data-value="3.0850"><span class="cell-value">3.0850</span></td>
    <td class="cell AC" data-name="bp" data-value="0.00" data-tag-target="bpNum"><span class="cell-value text-up">0.00</span><span class="san-icon m-l-2 san-icon-up"></span></td><td class="cell AC" data-name="dmiWghtdContraRate" data-value="3.0850"><span class="cell-value">3.0850</span></td><td class="cell AC" data-name="dmiTtlTradedAmnt" data-value="---"><span class="cell-value">---</span></td></tr>
    </tbody></table>
2,

'''

### 











































