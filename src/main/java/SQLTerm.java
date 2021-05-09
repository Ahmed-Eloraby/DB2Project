public class SQLTerm {
    String _strTableName,_strColumnName,_strOperator,_objValue;

    public SQLTerm(String _strTableName, String _strColumnName, String _strOperator, String _objValue) {
        this._strTableName = _strTableName;
        this._strColumnName = _strColumnName;
        this._strOperator = _strOperator;
        this._objValue = _objValue;
    }
}
