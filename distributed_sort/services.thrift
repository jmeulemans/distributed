struct KeyValue
{
    1: string name,
    2: string value
}
service ClientToServerService
{
    string sort(1: string filename),
    string getInfoFromServer(),
    bool setParameter(1: KeyValue parameter)
}
service ServerToNodeService
{
    string sort(1: string filename, 2: i32 offfset, 3: i32 len, 4: i32 id),
    string merge(1: list<string> filenames, 2: i32 id),
    bool kill(1: i32 id),
    void notifyOfJob(),
    bool setParameter(1: KeyValue parameter),
    string getInfoFromNode(),
    bool ping()
}
service NodeToServerService
{
    string registerNode(1: string ip, 2: i32 port),
}
