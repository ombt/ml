//
// base NN classes
//

//
// system includes
//
#include <assert.h>
#include <numeric>
#include <iostream>
#include <sstream>

//
// includes
//
#include "BaseNN.H"
#include "StdStringUtils.H"
#include "RegexMatch.H"

#undef DUMP
#define DUMP(X) \
    cout << "Line: " << __LINE__ << " " << #X << ": " << endl << (X) << endl

//
// activation functions for NN
//
const char *NNActivator::IDENTITY = "IDENTITY";
const char *NNActivator::SIGMOID = "SIGMOID";
const char *NNActivator::TANH = "TANH";
const char *NNActivator::RELU = "RELU";
const char *NNActivator::BINARY_STEP = "BINARY_STEP";
const char *NNActivator::ELU = "ELU";
const char *NNActivator::SOFT_EXPONENTIAL = "SOFT_EXPONENTIAL";

NNActivator::NNActivator(
    const char *name) : 
        _name(((name==NULL) ? IDENTITY : name))
{
}

NNActivator::NNActivator(
    const NNActivator &src) :
        _name(src._name)
{
}

NNActivator::~NNActivator()
{
}

NNActivator &
NNActivator::operator=(const NNActivator &rhs)
{
    if (this != &rhs)
    {
        _name = rhs._name;
    }
    return *this;
}

double 
NNActivator::f(double x)
{
    return x;
}

double
NNActivator::df(double x)
{
    return 1;
}

NNSigmoid::NNSigmoid() :
    NNActivator(SIGMOID)
{
}

NNSigmoid::NNSigmoid(const NNSigmoid &src) :
    NNActivator(src)
{
}

NNSigmoid::~NNSigmoid()
{
}

NNSigmoid &
NNSigmoid::operator=(const NNSigmoid &rhs)
{
    if (this != &rhs)
    {
        NNActivator::operator=(rhs);
    }
    return *this;
}

double 
NNSigmoid::f(double x)
{
    return 1.0/(1.0+exp(-x));
}

double 
NNSigmoid::df(double x)
{
    return 1;
}

NNTanh::NNTanh() :
    NNActivator(TANH)
{
}

NNTanh::NNTanh(const NNTanh &src) :
    NNActivator(src)
{
}

NNTanh::~NNTanh()
{
}

NNTanh &
NNTanh::operator=(const NNTanh &rhs)
{
    if (this != &rhs)
    {
        NNActivator::operator=(rhs);
    }
    return *this;
}

double 
NNTanh::f(double x)
{
    return tanh(x);
} 

double 
NNTanh::df(double x)
{
    return 1;
}

NNReLU::NNReLU() :
    NNActivator(RELU)
{
}

NNReLU::NNReLU(const NNReLU &src) :
    NNActivator(src)
{
}

NNReLU::~NNReLU()
{
}

NNReLU &
NNReLU::operator=(const NNReLU &rhs)
{
    if (this != &rhs)
    {
        NNActivator::operator=(rhs);
    }
    return *this;
}

double 
NNReLU::f(double x)
{
    return ( (x<0.0) ? 0.0 : x );
}

double 
NNReLU::df(double x)
{
    return 1;
}

NNBinaryStep::NNBinaryStep() :
    NNActivator(BINARY_STEP)
{
}

NNBinaryStep::NNBinaryStep(const NNBinaryStep &src) :
    NNActivator(src)
{
}

NNBinaryStep::~NNBinaryStep()
{
}

NNBinaryStep &
NNBinaryStep::operator=(const NNBinaryStep &rhs)
{
    if (this != &rhs)
    {
        NNActivator::operator=(rhs);
    }
    return *this;
}

double 
NNBinaryStep::f(double x)
{
    return ( (x<0) ? 0 : 1 );
}

double 
NNBinaryStep::df(double x)
{
    return 1;
}

NNELU::NNELU(double alpha) :
    NNActivator(ELU),
    _alpha(alpha)
{
}

NNELU::NNELU(const NNELU &src) :
    NNActivator(src),
    _alpha(src._alpha)
{
}

NNELU::~NNELU()
{
}

NNELU &
NNELU::operator=(const NNELU &rhs)
{
    if (this != &rhs)
    {
        NNActivator::operator=(rhs);

        _alpha = rhs._alpha;
    }
    return *this;
}

double 
NNELU::f(double x)
{
    if (x<0)
    {
        return _alpha*(exp(x)-1);
    }
    else
    {
        return x;
    }
}

double 
NNELU::df(double x)
{
    return 1;
}

NNSoftExponential::NNSoftExponential(double alpha) :
    NNActivator(SOFT_EXPONENTIAL),
        _alpha(alpha)
{
}

NNSoftExponential::NNSoftExponential(const NNSoftExponential &src) :
    NNActivator(src),
        _alpha(src._alpha)
{
}

NNSoftExponential::~NNSoftExponential()
{
}

NNSoftExponential &
NNSoftExponential::operator=(const NNSoftExponential &rhs)
{
    if (this != &rhs)
    {
        NNActivator::operator=(rhs);

        _alpha = rhs._alpha;
    }
    return *this;
}

double 
NNSoftExponential::f(double x)
{
    if (_alpha < 0)
    {
        return (-log(1-_alpha*(x+_alpha))/_alpha);
    }
    else if (_alpha == 0)
    {
        return x;
    }
    else
    {
        return ((exp(_alpha*x)-1)/_alpha + _alpha);
    }
}

double 
NNSoftExponential::df(double x)
{
    return 1;
}

//
// NN topology for NN
//
NNTopology::NNTopology() : _number_of_layers(0),
                           _number_of_neurons(0),
                           _neurons_per_layer(),
                           _weights(),
                           _layer_offsets(),
                           _pactivators()
{
}

NNTopology::NNTopology(const NNTopology &src) :
                           _number_of_layers(src._number_of_layers),
                           _number_of_neurons(src._number_of_neurons),
                           _neurons_per_layer(),
                           _weights(),
                           _layer_offsets(),
                           _pactivators()
{
    _neurons_per_layer.insert(_neurons_per_layer.begin(), 
                              src._neurons_per_layer.begin(),
                              src._neurons_per_layer.end());

    _weights.insert(_weights.begin(), 
                    src._weights.begin(),
                    src._weights.end());

    _layer_offsets.insert(_layer_offsets.begin(), 
                          src._layer_offsets.begin(),
                          src._layer_offsets.end());

    _pactivators.insert(_pactivators.begin(), 
                        src._pactivators.begin(),
                        src._pactivators.end());
}

void
NNTopology::load_from_file(const string &file_path)
{
    static PRegexMatch re_comment("^[ \t]*#");
    static PRegexMatch re_ws_only("^[ \t\r\n]*$");

    _neurons_per_layer.clear();
    _weights.clear();
    _layer_offsets.clear();

    string contents;
    read_file(file_path, contents);

    vector<string> lines;
    split(contents, lines, "\r\n", true);
    assert(lines.size() > 0);

    vector<string>::iterator it    = lines.begin();
    vector<string>::iterator itend = lines.end();

    States state = ReadNumberOfLayers;
    long neurons_per_layer_to_read = 0;
    long weights_to_read = 0;

    for (long iweight=1 ; (it!=itend) && (state!=End); ++it)
    {
        if (re_comment(it->c_str()) || re_ws_only(it->c_str()))
        {
            continue; // skip line
        }
        string value(trim(*it));

        switch (state)
        {
        case ReadNumberOfLayers:
        {
            istringstream(value) >> _number_of_layers;
            assert(_number_of_layers > 0);

            state = ReadNumberOfNeuronsPerLayer;
            neurons_per_layer_to_read = _number_of_layers;
            continue;
        }
        case ReadNumberOfNeuronsPerLayer:
        {
            vector<string> tokens;
            split(value, tokens, " \t", true);
            assert(tokens.size() >= 2);

            long number_of_neurons = 0;
            istringstream(trim(tokens[0])) >> number_of_neurons;

            assert(number_of_neurons > 0);
            _neurons_per_layer.push_back(number_of_neurons);

            NNActivatorPtr pactivator = NULL;
            string act_type(trim(tokens[1]));
            to_uppercase(act_type);
            if (act_type == NNActivator::IDENTITY)
            {
                pactivator = new NNActivator;
            }
            else if (act_type == NNActivator::SIGMOID)
            {
                pactivator = new NNSigmoid;
            }
            else if (act_type == NNActivator::TANH)
            {
                pactivator = new NNTanh;
            }
            else if (act_type == NNActivator::RELU)
            {
                pactivator = new NNReLU;
            }
            else if (act_type == NNActivator::BINARY_STEP)
            {
                pactivator = new NNBinaryStep;
            }
            else if (act_type == NNActivator::ELU)
            {
                assert(tokens.size() == 3);
                double alpha = 0.0;
                istringstream(tokens[2]) >> alpha;
                pactivator = new NNELU(alpha);
            }
            else if (act_type == NNActivator::SOFT_EXPONENTIAL)
            {
                assert(tokens.size() == 3);
                double alpha = 0.0;
                istringstream(tokens[2]) >> alpha;
                pactivator = new NNSoftExponential(alpha);
            }
            else
            {
                cout << "Unknown activator: " << act_type << endl;
                assert(0);
            }
            _pactivators.push_back(pactivator);

            if (--neurons_per_layer_to_read <= 0)
            {
                weights_to_read = 0;

                NeuronsPerLayerCIt nit    = _neurons_per_layer.begin();
                NeuronsPerLayerCIt nitend = _neurons_per_layer.end();
                for (long layer=1, neurons = *nit++; nit!=nitend; ++nit, ++layer)
                {
                    long weights_in_layer = (neurons+1)*(*nit);
#ifdef LOCAL_DEBUG
                    cout << "Layer: " << layer 
                         << ", Neurons in Layer + Bias: " << (neurons+1)
                         << ", Neurons in Next Layer: " << *nit
                         << ", Weights in Layer: " << weights_in_layer 
                         << endl;
#endif
                    weights_to_read += weights_in_layer;
                    neurons = *nit;
                }
#ifdef LOCAL_DEBUG
                cout << "Weights to Read: " << weights_to_read << endl;
#endif

                state = ReadWeightsPerLayer;
            }
            continue;
        }
        case ReadWeightsPerLayer:
        {
            double weight = 0.0;
            istringstream(value) >> weight;
            _weights.push_back(weight);

#ifdef LOCAL_DEBUG
            cout << "No: " << iweight++ << ", Weight: " << weight << endl;
#endif

            if (--weights_to_read <= 0)
            {
                state = End;
            }
            continue;
        }
        case End:
        {
            continue;
        }
        default:
        {
            assert(0);
        }
        }
    }

    assert(state == End);

    _number_of_neurons = accumulate(_neurons_per_layer.begin(),
                                    _neurons_per_layer.end(), 0);

    long total = 0;
    _layer_offsets.push_back(total);

    NeuronsPerLayerCIt nit    = _neurons_per_layer.begin();
    NeuronsPerLayerCIt nitend = _neurons_per_layer.end();
    for (long previous=*nit++ ; nit!=nitend; ++nit)
    {
        total += (previous+1)*(*nit); // one bias weight per layer
        _layer_offsets.push_back(total);
        previous=*nit;
    }
}

NNTopology::~NNTopology()
{
}

NNTopology &
NNTopology::operator=(const NNTopology &rhs)
{
    if (this != &rhs)
    {
        _number_of_layers = rhs._number_of_layers;
        _number_of_neurons = rhs._number_of_neurons;

        _neurons_per_layer.clear();
        _weights.clear();
        _layer_offsets.clear();
        _pactivators.clear();

        _neurons_per_layer.insert(_neurons_per_layer.begin(), 
                                  rhs._neurons_per_layer.begin(),
                                  rhs._neurons_per_layer.end());

        _weights.insert(_weights.begin(), 
                        rhs._weights.begin(),
                        rhs._weights.end());

        _layer_offsets.insert(_layer_offsets.begin(), 
                        rhs._layer_offsets.begin(),
                        rhs._layer_offsets.end());

        _pactivators.insert(_pactivators.begin(), 
                            rhs._pactivators.begin(),
                            rhs._pactivators.end());
    }

    return *this;
}

//
// base neural net neuron
//
NNNeuron::NNNeuron() :
    _value(numeric_limits<double>::quiet_NaN()),
    _weights()
{
}

NNNeuron::NNNeuron(const NNNeuron &src) :
    _value(src._value),
    _weights()
{
    _weights.insert(_weights.begin(), 
                    src._weights.begin(), 
                    src._weights.end());
}

NNNeuron::~NNNeuron()
{
    _weights.clear();
}

NNNeuron &
NNNeuron::operator=(const NNNeuron &rhs)
{
    if (this != &rhs)
    {
        _value = rhs._value;

        _weights.clear();
        _weights.insert(_weights.begin(), 
                        rhs._weights.begin(), 
                        rhs._weights.end());
    }
    return *this;
}

void 
NNNeuron::append(double weight)
{
    _weights.push_back(weight);
}


//
// base neural net layer class
//
NNLayer::NNLayer(NNActivatorPtr pactivator) :
    _pactivator(pactivator),
    _neurons()
{
    assert(_pactivator != NULL);
}

NNLayer::NNLayer(const NNLayer &src) :
    _pactivator(src._pactivator),
    _neurons()
{
    _neurons.insert(_neurons.begin(), 
                    src._neurons.begin(), 
                    src._neurons.end());
}

NNLayer::~NNLayer()
{
    _neurons.clear();
}

NNLayer &
NNLayer::operator=(const NNLayer &rhs)
{
    if (this != &rhs)
    {
        _pactivator = rhs._pactivator;

        _neurons.clear();
        _neurons.insert(_neurons.begin(), 
                        rhs._neurons.begin(), 
                        rhs._neurons.end());
    }
    return *this;
}

void
NNLayer::append(NNNeuron *pnnn)
{
    _neurons.push_back(ExtUseCntPtr<NNNeuron>(pnnn));
}

//
// base neural net class
//
NeuralNet::NeuralNet(const NNTopology &topology) :
    _topology(topology),
    _layers()
{
    load_topology();
}

NeuralNet::NeuralNet(const NeuralNet &src) :
    _topology(src._topology)
{
    _layers.insert(_layers.begin(), 
                    src._layers.begin(), 
                    src._layers.end());
}

NeuralNet::~NeuralNet()
{
    _layers.clear();
}

NeuralNet &
NeuralNet::operator=(const NeuralNet &rhs)
{
    if (this != &rhs)
    {
        _layers.clear();
        _layers.insert(_layers.begin(), 
                       rhs._layers.begin(), 
                       rhs._layers.end());
    }
    return *this;
}

void
NeuralNet::append(NNLayer *pnnl)
{
    _layers.push_back(ExtUseCntPtr<NNLayer>(pnnl));
}

void
NeuralNet::load_topology()
{
    long max_layers = _topology.number_of_layers();
    for (long layer=0; layer<max_layers; ++layer)
    {
        long neurons_in_layer = _topology.neurons_per_layer()[layer];
        cout << "Layer: " << (layer+1) 
             << ", Neurons: " << neurons_in_layer 
             << endl;

        //
        // new layer
        //
        NNLayer *pl = new NNLayer(_topology.activator(layer));

        long next_layer = layer + 1;
        if (next_layer < max_layers)
        {
            //
            // get weights for this layer connecting to next layer
            //
            long neurons_in_next_layer = _topology.neurons_per_layer()[next_layer];
            for (long nil=0; nil<=neurons_in_layer; ++nil)
            {
                //
                // new neuron in layer. remember the first neuron, 
                //
                NNNeuron *pn = new NNNeuron;

                for (long ninl=0; ninl<neurons_in_next_layer; ++ninl)
                {
                    double weight = _topology.weight(layer, nil, ninl);
                    if (nil>0)
                    {
                        cout << "Layer: " << (layer+1)
                             << ", This Layer Neuron: " << nil
                             << ", Next Layer Neuron: " << (ninl+1)
                             << ", Weight: " << weight
                             << endl;
                    }
                    else
                    {
                        cout << "Layer: " << (layer+1)
                             << ", This Layer Bias: " << 1
                             << ", Next Layer Neuron: " << (ninl+1)
                             << ", Weight: " << weight
                             << endl;
                    }

                    //
                    // append weight to this neuron
                    //
                    pn->append(weight);
                }

                //
                // append neuron to this layer (first neuron in the layer
                // is the bias term).
                //
                pl->append(pn);
            }
        }
        else
        {
            //
            // output layer - no weights
            //
            for (long nil=0; nil<=neurons_in_layer; ++nil)
            {
                //
                // new neuron
                //
                NNNeuron *pn = new NNNeuron;

                //
                // append neuron to this layer
                //
                pl->append(pn);
            }
        }

        //
        // append layer to this NN
        //
        append(pl);
    }
}

void
NeuralNet::apply(const NNVector &inv, NNVector &outv)
{
    assert(layer(0).number_of_neurons() == inv.size());

    //
    // set all bias terms to one
    //
DUMP(*this);
    for (int ilayer=0; ilayer<number_of_layers(); ++ilayer)
    {
        layer(ilayer).neuron(0)._value = 1.0;
    }

    for (int ineuron=0; ineuron<layer(0).number_of_neurons(); ++ineuron)
    {
        layer(0).neuron(ineuron+1)._value = inv[ineuron];
    }

    for (int ilayer=0; ilayer<number_of_layers(); ++ilayer)
    {
        NNLayer &this_layer = layer(ilayer);

        int inext_layer = ilayer + 1;
        if (inext_layer < number_of_layers())
        {
            NNLayer &next_layer = layer(inext_layer);
            for (int inl=1; inl<=next_layer.number_of_neurons(); ++inl)
            {
                NNNeuron &next_layer_neuron = next_layer[inl];
                next_layer_neuron._value = 0.0;

                for (int il=0; il<this_layer.number_of_nodes(); ++il)
                {
                    NNNeuron &this_layer_neuron = this_layer[il];
                    next_layer_neuron._value += 
                        this_layer_neuron._value*this_layer_neuron.weight(inl-1);
                }

                next_layer_neuron._value = 
                    next_layer.activate(next_layer_neuron._value);
            }
        }
        else
        {
            outv.clear();
            for (int il=1; il<=this_layer.number_of_neurons(); ++il)
            {
                NNNeuron &this_layer_neuron = this_layer[il];
                outv.push_back(this_layer_neuron._value);
            }
        }
    }
DUMP(*this);
}

//
// I/O operators
//
ostream &
operator<<(ostream &os, const NNTopology &src)
{
    os << endl << "Number of Layers: " << src._number_of_layers << endl;
    os << endl << "Number of Neurons: " << src._number_of_neurons << endl;

    os << endl << "Neurons per Layer: " << endl;
    NeuronsPerLayerCIt npl_it    = src._neurons_per_layer.begin();
    NeuronsPerLayerCIt npl_itend = src._neurons_per_layer.end();
    for (int layer=1 ; npl_it!=npl_itend; ++npl_it, ++layer)
    {
        os << "Layer: " << layer << ", Number of Neurons: " << *npl_it << endl;
    }

    os << endl << "Weights: " << endl;
    WeightsCIt w_it    = src._weights.begin();
    WeightsCIt w_itend = src._weights.end();
    for ( ; w_it!=w_itend; ++w_it)
    {
        os << "Weight: " << *w_it << endl;
    }

    os << endl << "Layer Offsets: " << endl;
    LayerOffsetsCIt lo_it    = src._layer_offsets.begin();
    LayerOffsetsCIt lo_itend = src._layer_offsets.end();
    for (int layer=1 ; lo_it!=lo_itend; ++layer, ++lo_it)
    {
        os << "Layer: " << layer << ", Layer Offset: " << *lo_it << endl;
    }

    return os;
}

ostream &
operator<<(ostream &os, const NNNeuron &src)
{
    os << "\t\tValue: " << src._value << endl;

    WeightsCIt it    = src._weights.begin();
    WeightsCIt itend = src._weights.end();
    for (int weight=1; it!=itend; ++it, ++weight)
    {
        os << "\t\tWeight: " << weight << " ==>> " << *it << endl;
    }
    return os;
}

ostream &
operator<<(ostream &os, const NNLayer &src)
{
    NNNeuronsCIt it    = src._neurons.begin();
    NNNeuronsCIt itend = src._neurons.end();
    for (int neuron=0; it!=itend; ++it, ++neuron)
    {
        os << "\tNeuron: " << neuron << endl
           << **it << endl;
    }
    return os;
}

ostream &
operator<<(ostream &os, const NeuralNet &src)
{
    NNLayersCIt it    = src._layers.begin();
    NNLayersCIt itend = src._layers.end();
    for (int layer=1; it!=itend; ++it, ++layer)
    {
        os << "Layer: " << layer << endl
           << **it << endl;
    }
    return os;
}

ostream &
operator<<(ostream &os, const NNVector &nnv)
{
    os << "( ";
    if (nnv.size() > 0)
    {
        int i=0;
        for ( ; i<(nnv.size()-1); ++i)
        {
            os << nnv[i] << ",";
        }
        os << nnv[i];
    }
    os << " )";
    return os;
}

