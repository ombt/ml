#ifndef __BASE_NN_H
#define __BASE_NN_H

//
// system includes
//
#include <vector>
#include <string>
#include <iostream>
#include <limits>
#include <math.h>

//
// local includes
//
#include "ExtUseCntPtr.H"

using namespace std;

//
// NN-related types
//
typedef double Weight;

typedef vector<Weight> Weights;
typedef Weights::iterator WeightsIt;
typedef Weights::const_iterator WeightsCIt;

typedef vector<long> NeuronsPerLayer;
typedef NeuronsPerLayer::iterator NeuronsPerLayerIt;
typedef NeuronsPerLayer::const_iterator NeuronsPerLayerCIt;

typedef vector<long> LayerOffsets;
typedef LayerOffsets::iterator LayerOffsetsIt;
typedef LayerOffsets::const_iterator LayerOffsetsCIt;

// base neural net classes
//
typedef vector<double> NNVector;
typedef NNVector::iterator NNVectorIt;
typedef NNVector::const_iterator NNVectorCIt;

ostream &operator<<(ostream &os, const NNVector &nnv);

class NNError
{
public: // member functions
    //
    // ctors, dtor and assignment
    //
    NNError();
    NNError(const NNError &src);
    virtual ~NNError();
    NNError &operator=(const NNError &rhs);

    //
    // operator() - calculate error
    //
    virtual double operator()(const NNVector &expected, 
                              const NNVector &actual) const  = 0;
};

class NNAbsoluteValue : public NNError
{
public: // member functions
    //
    // ctors, dtor and assignment
    //
    NNAbsoluteValue();
    NNAbsoluteValue(const NNAbsoluteValue &src);
    virtual ~NNAbsoluteValue();
    NNAbsoluteValue &operator=(const NNAbsoluteValue &rhs);

    //
    // operator() - calculate error
    //
    virtual double operator()(const NNVector &expected, 
                              const NNVector &actual) const;
};

class NNChiSquared : public NNError
{
public: // member functions
    //
    // ctors, dtor and assignment
    //
    NNChiSquared();
    NNChiSquared(const NNChiSquared &src);
    virtual ~NNChiSquared();
    NNChiSquared &operator=(const NNChiSquared &rhs);

    //
    // operator() - calculate error
    //
    virtual double operator()(const NNVector &expected, 
                              const NNVector &actual) const;
};

class NNActivator
{
public: // member functions
    //
    // ctors, dtor and assignment
    //
    NNActivator(const char *name = NULL);
    NNActivator(const NNActivator &src);
    virtual ~NNActivator();
    NNActivator &operator=(const NNActivator &rhs);

    //
    // f - activation function
    // fp - first derivative of activation function
    //
    virtual double f(double x);
    virtual double df(double x);

public: // member data

    static const char *IDENTITY;
    static const char *SIGMOID;
    static const char *TANH;
    static const char *RELU;
    static const char *BINARY_STEP;
    static const char *ELU;
    static const char *SOFT_EXPONENTIAL;

protected: // member data
    string _name;
};

typedef NNActivator * NNActivatorPtr;
typedef vector<ExtUseCntPtr<NNActivator> > NNActivators;
typedef NNActivators::iterator NNActivatorsIt;
typedef NNActivators::const_iterator NNActivatorsCIt;

class NNSigmoid : public NNActivator
{
public: // member functions
    //
    // ctors, dtor and assignment
    //
    NNSigmoid();
    NNSigmoid(const NNSigmoid &src);
    ~NNSigmoid();
    NNSigmoid &operator=(const NNSigmoid &rhs);

    //
    // f - activation function
    // fp - first derivative of activation function
    //
    virtual double f(double x);
    virtual double df(double x);
};

class NNTanh : public NNActivator
{
public: // member functions
    //
    // ctors, dtor and assignment
    //
    NNTanh();
    NNTanh(const NNTanh &src);
    ~NNTanh();
    NNTanh &operator=(const NNTanh & );

    //
    // f - activation function
    // fp - first derivative of activation function
    //
    virtual double f(double x);
    virtual double df(double x);
};

class NNReLU : public NNActivator
{
public: // member functions
    //
    // ctors, dtor and assignment
    //
    NNReLU();
    NNReLU(const NNReLU &src);
    ~NNReLU();
    NNReLU &operator=(const NNReLU & );

    //
    // f - activation function
    // fp - first derivative of activation function
    //
    virtual double f(double x);
    virtual double df(double x);
};

class NNBinaryStep : public NNActivator
{
public: // member functions
    //
    // ctors, dtor and assignment
    //
    NNBinaryStep();
    NNBinaryStep(const NNBinaryStep &src);
    ~NNBinaryStep();
    NNBinaryStep &operator=(const NNBinaryStep &rhs);

    //
    // f - activation function
    // fp - first derivative of activation function
    //
    virtual double f(double x);
    virtual double df(double x);
};

class NNELU : public NNActivator
{
public: // member functions
    //
    // ctors, dtor and assignment
    //
    NNELU(double alpha = 1.0);
    NNELU(const NNELU &src);
    ~NNELU();
    NNELU &operator=(const NNELU &rhs);

    //
    // f - activation function
    // fp - first derivative of activation function
    //
    virtual double f(double x);
    virtual double df(double x);

protected: // member data
    double _alpha;
};

class NNSoftExponential : public NNActivator
{
public: // member functions
    //
    // ctors, dtor and assignment
    //
    NNSoftExponential(double alpha = 1.0);
    NNSoftExponential(const NNSoftExponential &src);
    ~NNSoftExponential();
    NNSoftExponential &operator=(const NNSoftExponential &rhs);

    //
    // f - activation function
    // fp - first derivative of activation function
    //
    virtual double f(double x);
    virtual double df(double x);

protected: // member data
    double _alpha;
};

class NNTopology
{
protected: // member data types
    enum States
    {
        ReadNumberOfLayers = 1,
        ReadNumberOfNeuronsPerLayer,
        ReadWeightsPerLayer,
        End,
    } ;

public: // member functions
    //
    // ctors and dtor
    //
    NNTopology();
    NNTopology(const NNTopology &src);
    virtual ~NNTopology();

    //
    // load_from_file - read topology file
    // operator= - assignment operator
    //
    void load_from_file(const string &file_path);
    NNTopology &operator=(const NNTopology &rhs);

    //
    // accessors
    //
    inline long number_of_layers() const
    {
        return _number_of_layers;
    }
    inline const NeuronsPerLayer &neurons_per_layer() const
    {
        return _neurons_per_layer;
    }
    inline const Weights &weights() const
    {
        return _weights;
    }
    inline const Weight &weight(long layer, long n, long next_n)
    {
        return _weights[_layer_offsets[layer] +
                        n * _neurons_per_layer[layer + 1] +
                        next_n];
    }
    inline NNActivatorPtr activator(long layer)
    {
        assert((0<=layer) && (layer< _pactivators.size()));
        return _pactivators[layer];
    }

    //
    // i/o operator
    //
    friend ostream &operator<<(ostream &os, const NNTopology &src);

protected: // member data
    long _number_of_layers;
    long _number_of_neurons;
    NeuronsPerLayer _neurons_per_layer;
    Weights _weights;
    LayerOffsets _layer_offsets;
    NNActivators _pactivators;
};

class NNNeuron
{
public: // member functions
    //
    // ctors and dtor
    //
    NNNeuron();
    NNNeuron(const NNNeuron &src);
    virtual ~NNNeuron();

    //
    // weight - access a weight
    // number_of_weights - number of weights
    // operator= - assignment operator
    // append - append a weight
    //
    inline Weight &weight(long neuron)
    {
        return _weights[neuron];
    }
    inline long number_of_weights() const
    {
        return _weights.size();
    }
    NNNeuron &operator=(const NNNeuron &rhs);
    virtual void append(double weight);

    //
    // i/o operator
    //
    friend ostream &operator<<(ostream &os, const NNNeuron &src);

public: // member data
    mutable double _value;

protected: // member data
    Weights _weights;
};

typedef vector<ExtUseCntPtr<NNNeuron> > NNNeurons;
typedef NNNeurons::iterator NNNeuronsIt;
typedef NNNeurons::const_iterator NNNeuronsCIt;

class NNLayer
{
public: // member functions
    //
    // ctors and dtor
    //
    NNLayer(NNActivatorPtr pactivator = NULL);
    NNLayer(const NNLayer &src);
    virtual ~NNLayer();

    //
    // neuron - access NN in a layer
    // number_of_neurons - number of neurons in layer
    // number_of_nodes - number of neurons + bias in layer
    // operator= - assignment operator
    // append - append a neueon
    // activate - run activator on a value
    //
    inline NNNeuron &operator[](long neuron)
    {
        return *_neurons[neuron];
    }
    inline NNNeuron &neuron(long neuron)
    {
        return *_neurons[neuron];
    }
    inline long number_of_neurons() const
    {
        return _neurons.size()-1; // do not include the bias node
    }
    inline long number_of_nodes() const
    {
        return _neurons.size(); // include the bias node at index 0
    }
    inline double activate(double x)
    {
        return _pactivator->f(x);
    }

    NNLayer &operator=(const NNLayer &rhs);
    virtual void append(NNNeuron *pnnn);

    //
    // i/o operator
    //
    friend ostream &operator<<(ostream &os, const NNLayer &src);

protected: // member data
    NNActivatorPtr _pactivator;
    NNNeurons _neurons;
};

typedef vector<ExtUseCntPtr<NNLayer> > NNLayers;
typedef NNLayers::iterator NNLayersIt;
typedef NNLayers::const_iterator NNLayersCIt;

class NeuralNet
{
public: // member functions
    //
    // ctors and dtor
    //
    NeuralNet(const NNTopology &topology);
    NeuralNet(const NeuralNet &src);
    virtual ~NeuralNet();

    //
    // neuron - access NN in a layer
    // layer - access a layer
    // number_of_layers - return the number of layers in NN
    // operator= - assignment operator
    // append - append a new layer
    // load_topology - create NN from topology
    //
    inline NNNeuron &neuron(long layer, long neuron)
    {
        return (*_layers[layer])[neuron];
    }
    inline const NNNeuron &neuron(long layer, long neuron) const
    {
        return (*_layers[layer])[neuron];
    }
    inline NNLayer &layer(long layer)
    {
        return *_layers[layer];
    }
    inline const NNLayer &layer(long layer) const
    {
        return *_layers[layer];
    }
    inline NNLayer &operator[](long layer)
    {
        return *_layers[layer];
    }
    inline const NNLayer &operator[](long layer) const
    {
        return *_layers[layer];
    }
    inline long number_of_layers() const
    {
        return _layers.size();
    }
    NeuralNet &operator=(const NeuralNet &rhs);
    virtual void append(NNLayer *pnnl);
    virtual void load_topology();

    //
    // apply - apply NN to an input and generate output
    // error - calculate error term for actual out versus expected out
    //
    void apply(const NNVector &inv, NNVector &outv);
    inline double error(const NNVector &actual, 
                        const NNVector &expected, 
                        const NNError &ftor)
    {
        return ftor(actual, expected);
    }

    //
    // i/o operator
    //
    friend ostream &operator<<(ostream &os, const NeuralNet &src);

private: // not allowed
    NeuralNet();

protected: // member data
    NNTopology _topology;
    NNLayers _layers;
};

#endif

