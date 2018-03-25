#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <map>
#include <sstream>
#include <algorithm>
#include <vector>

#define BUF_SIZE 512
#define LEN_TERM 1
#define LEN_TS LEN_TERM + 8
#define LEN_MSG_TYPE LEN_TERM + 1
#define LEN_ORDER_ID LEN_TERM + 12
#define LEN_EXEC_ID LEN_TERM + 12
#define LEN_SIDE_IND LEN_TERM + 1
#define LEN_NO_SHARES LEN_TERM + 6
#define LEN_SYMBOL_SHORT LEN_TERM + 6
#define LEN_SYMBOL_LONG LEN_TERM + 8
#define LEN_PRICE LEN_TERM + 10
#define LEN_DISPLAY LEN_TERM + 1
#define LEN_PART_ID LEN_TERM + 4

#define MSG_TYPE_ADD_ORDER_SHORT 'A'
#define MSG_TYPE_ADD_ORDER_LONG 'd'
#define MSG_TYPE_ORDER_EXEC 'E'
#define MSG_TYPE_ORDER_CANCEL 'X'
#define MSG_TYPE_TRADE_SHORT 'P'
#define MSG_TYPE_TRADE_LONG 'r'

using namespace std;

class Order;

enum MsgType { LONG, SHORT };


typedef std::map<std::string, Order*> OrderPool;
typedef std::map<std::string, int> TradeVolume;

struct Order
{
    string orderId;
    char   side;
    int    shares;
    string symbol;
    float  price;
    char   display;
    string part_id;
};

class OrderManager
{
    OrderPool m_orderPool;
    TradeVolume m_tradeVolume;

public:

    Order* parseAddOrder( const char *msg, MsgType t );
    void addOrder( const char* buf, MsgType t );

    void processExecution( const char *msg );
    void processCancel( const char *msg );
    void processTrade( const char *msg, MsgType t );
    void printTopVolume( int n );

    void incrementVolume( const string &symbol, int volume );
};

void OrderManager::printTopVolume(int n)
{
    // print the top n symbols with the highest volume
    vector<pair<string,int>> v(n);
    partial_sort_copy( m_tradeVolume.begin(), m_tradeVolume.end(), v.begin(), v.end(),
        []( const pair<string, int> &l, const pair<string, int> &r ){ return l.second > r.second; } );

    cout << "Top "<< n << " Symbols by Volume"<< endl;
    for ( auto it = v.begin(); it != v.end(); ++it)
        cout << it->first << " - " << it->second << endl;
}

void OrderManager::incrementVolume( const string &symbol, int shares )
{
    auto it = m_tradeVolume.find(symbol);
    if ( it != m_tradeVolume.end() )
        m_tradeVolume[symbol] += shares;
    else
        m_tradeVolume[symbol] = shares;
}

void OrderManager::processTrade(const char *msg, MsgType t )
{
    char buf[BUF_SIZE];
    stringstream ss(msg);

    ss.get(buf, LEN_ORDER_ID);
    string orderId = buf;

    ss.get(buf, LEN_SIDE_IND);
    char side = buf[0];

    ss.get(buf, LEN_NO_SHARES);
    int shares = stoi(buf);

    if (t == MsgType::SHORT)
        ss.get(buf, LEN_SYMBOL_SHORT);
    else
        ss.get(buf, LEN_SYMBOL_LONG);

    string symbol = buf;

    ss.get(buf, LEN_PRICE);
    string s(buf);
    s.insert(6,".");
    float price = stof(s);

    ss.get(buf, LEN_EXEC_ID);
    string execId = buf;

    this->incrementVolume( symbol, shares );
}

void OrderManager::processCancel(const char *msg)
{
    char buf[BUF_SIZE];
    stringstream ss(msg);

    ss.get(buf, LEN_ORDER_ID);
    string orderId = buf;

    ss.get(buf, LEN_NO_SHARES);
    int shares = stoi(buf);

    auto it = m_orderPool.find( orderId );
    if (it != m_orderPool.end() )
    {
        Order* order = it->second;
        order->shares -= shares; // decrement the no. of outstanding shares
    }
}

void OrderManager::processExecution(const char *msg)
{
    char buf[BUF_SIZE];
    stringstream ss(msg);

    ss.get(buf, LEN_ORDER_ID);
    string orderId = buf;

    ss.get(buf, LEN_NO_SHARES);
    int shares = stoi(buf);

    ss.get(buf, LEN_EXEC_ID);
    string execId = buf;

    auto it = m_orderPool.find( orderId );
    if (it != m_orderPool.end() )
    {
        Order* order = it->second;
        order->shares -= shares; // decrement the no. of outstanding shares

        // now add to trade volume summary
        this->incrementVolume( order->symbol, shares );
    }
    else
        cout << "Warning! " << orderId << " not found " << endl;

}

void OrderManager::addOrder( const char *msg, MsgType t )
{
    Order* order = this->parseAddOrder( msg, t );
    m_orderPool[order->orderId] = order;
}

Order* OrderManager::parseAddOrder( const char *msg, MsgType t )
{
    char buf[BUF_SIZE];
    stringstream ss(msg);

    ss.get(buf, LEN_ORDER_ID);

    Order* order = new Order;
    order->orderId = buf;

    ss.get(buf, LEN_SIDE_IND);
    order->side = buf[0];

    ss.get(buf, LEN_NO_SHARES);
    order->shares = stoi(buf);

    if (t == MsgType::SHORT)
        ss.get(buf, LEN_SYMBOL_SHORT);
    else
        ss.get(buf, LEN_SYMBOL_LONG);

    order->symbol = buf;

    ss.get(buf, LEN_PRICE);
    string s(buf);
    s.insert(6,".");
    order->price = stof(s);

    ss.get(buf, LEN_DISPLAY);
    order->display = buf[0];

    if ( t == MsgType::LONG )
    {
        ss.get(buf, LEN_PART_ID);
        order->part_id = buf;
    }

    return order;
}

int main() {

    ifstream ifs ( "/Users/unclechu/Dropbox/Code/machine_learning/load_patch_data/pitch_example_data");
    //ifstream ifs ( "/Users/unclechu/Dropbox/Code/machine_learning/load_patch_data/small.dat");

    string line;
    char buf[BUF_SIZE];

    OrderManager orderManager;
    int cnt = 0;
    while( ifs.good() )
    {
        // ignore any Trade Break and long messages (‘B’, ‘r’, ‘d’).

        ifs.ignore(1); // this skips the S in front of every line
        ifs.get(buf, LEN_TS );

        if ( ifs.eof() and ifs.gcount() == 0)
            break;

        ifs.get(buf, LEN_MSG_TYPE);

        if( buf[0] == MSG_TYPE_ADD_ORDER_SHORT )
        {
            // long msgs are ignored.
            ifs.get( buf, BUF_SIZE );
            orderManager.addOrder( buf, MsgType::SHORT );
        }
        else if ( buf[0] == MSG_TYPE_ORDER_EXEC )
        {
            ifs.get( buf, BUF_SIZE );
            orderManager.processExecution(buf);
        }
        else if ( buf[0] == MSG_TYPE_ORDER_CANCEL )
        {
            ifs.get( buf, BUF_SIZE );
            orderManager.processCancel(buf);
        }
        else if ( buf[0] == MSG_TYPE_TRADE_SHORT )
        {
            ifs.get( buf, BUF_SIZE );
            orderManager.processTrade(buf, MsgType::SHORT);
        }
        else
        {
            // ignored messages. read till eol
            ifs.get( buf, BUF_SIZE );
            cout << endl;
        }

        ifs.ignore(1); // this reads past the \n

    }

    ifs.close();
    orderManager.printTopVolume(10);

    return 0;
}