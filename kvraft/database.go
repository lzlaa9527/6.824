package kvraft
import . "6.824/common"


type Database map[string]string

func (d Database) Put(k, v string) PutAppendReply {
	d[k] = v
	return PutAppendReply{Err: OK}
}

func (d Database) Get(k string) GetReply {

	r := new(GetReply)
	if _, ok := d[k]; !ok {
		r.Err = ErrNoKey
	} else {
		r.Value = d[k]
		r.Err = OK
	}
	return *r
}

func (d Database) Append(k, v string) PutAppendReply {

	if _, ok := d[k]; !ok {
		d[k] = v
	} else {
		d[k] += v
	}
	return PutAppendReply{OK}
}
