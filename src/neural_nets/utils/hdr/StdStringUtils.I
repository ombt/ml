
template <class ContainerType>
void
split(const string &str, 
      ContainerType &tokens,
      const string &delimiters, 
      bool trim_empty)
{
    string::size_type pos;
    string::size_type last_pos = 0;

    while (true)
    {
        pos = str.find_first_of(delimiters, last_pos);
        if (pos == std::string::npos)
        {
            pos = str.length();

            if (pos != last_pos || !trim_empty)
            {
                tokens.push_back(
                    vector<string>::value_type(str.data()+last_pos,
                   (vector<string>::value_type::size_type)pos-last_pos));
            }
            break;
        }
        else
        {
            if (pos != last_pos || !trim_empty)
            {
                tokens.push_back(
                    vector<string>::value_type(str.data()+last_pos,
                   (vector<string>::value_type::size_type)pos-last_pos ));
            }
        }
        last_pos = pos + 1;
    }
}

