package common;

public class SymbolConstants {

    public static final String SYMBOL_DH = ",";// 逗号常量
    public static final String SYMBOL_TAB = "\t";
    public static final String SYMBOL_DHG = "-";//短横杠常量
    public static final String SYMBOL_XHX = "_";// 下划线常量
    public static final String SYMBOL_SINGLE_YH = "'";// 单引号常量
    public static final String SYMBOL_ZHX = "|";// 中划线常量
    public static final String SYMBOL_MH = ":";//冒号常量
    public static final String SYMBOL_EMPTY_STRING = "";// 空白字符串
    public static final String SYMBOL_HH = "\n";//换行字符串
    /** 成功状态 **/
    public static final Integer STATUS_SUCCESS = 1;// 成功状态
    public static final Integer STATUS_ERROR = 0;// 失败状态
    public static final Integer STATUS_EXCEPTION = -1;// 异常状态
    /** 字符串值为null **/
    public static final String nullString = "null";
    public static final String NAString = "N/A"; // 存放一个简短的字符串对象，表示当DB里没数据时候的缩写
    public static final String SEPERATOR_001_STR = new String(new byte[] { 1 });//byte-001的分隔符,通常用于行分隔
}
