package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	pb "github.com/grpc-kubernate/proto"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func init() {
	DatabaseConnection()
}

var DB *gorm.DB
var err error

type Movie struct {
	ID        string `gorm:"primarykey"`
	Title     string
	Genre     string
	CreateAt  time.Time `gorm:"autoCreateTime:false"`
	UpdatedAt time.Time `gorm:"autoUpdateTime:false"`
}

func DatabaseConnection() {
	host := "localhost"
	port := 5432
	dbName := "postgres"
	dbUser := "postgres"
	password := 123456789
	dns := fmt.Sprintf("host=%s port=%d user=%s dbname=%s password=%d sslmode=disable",
		host,
		port,
		dbUser,
		dbName,
		password,
	)
	DB, err = gorm.Open(postgres.Open(dns), &gorm.Config{})
	DB.AutoMigrate(&Movie{})
	if err != nil {
		log.Fatal("Error to connecting database...!")
	}
	fmt.Println("Database connection successfull...!")

}

var (
	port = flag.Int("port", 50051, "gRPC server port")
)

type server struct {
	pb.UnimplementedMovieServiceServer
}

func (*server) CreateMovie(ctx context.Context, req *pb.CreateMovieRequest) (*pb.CreateMovieResponse, error) {
	fmt.Println("Create Movie")
	movie := req.GetMovie()
	movie.Id = uuid.New().String()
	data := Movie{
		ID:    movie.GetId(),
		Title: movie.GetTitle(),
		Genre: movie.GetGenre(),
	}
	resp := DB.Create(&data)
	if resp.RowsAffected == 0 {
		return nil, errors.New("movie creation unsucessfull")
	}
	return &pb.CreateMovieResponse{
		Movie: &pb.Movie{Id: movie.GetId(),
			Title: movie.GetTitle(),
			Genre: movie.GetGenre(),
		},
	}, nil

}

func (*server) GetMovie(ctx context.Context, req *pb.ReadMovieRequest) (*pb.ReadMovieResponse, error) {
	fmt.Println("Read  Movie", req.GetId())
	var movie Movie
	res := DB.Find(&movie, "id=?", req.GetId())
	if res.RowsAffected == 0 {
		return nil, errors.New("movie not found")
	}
	return &pb.ReadMovieResponse{
		Movie: &pb.Movie{
			Id:    movie.ID,
			Title: movie.Title,
			Genre: movie.Genre,
		},
	}, nil
}

func (*server) GetMovies(ctx context.Context, req *pb.ReadMoviesRequest) (*pb.ReadMoviesResponse, error) {
	fmt.Println("Read Movies")
	movies := []*pb.Movie{}
	res := DB.Find(&movies)
	if res.RowsAffected == 0 {
		return nil, errors.New("Movie not found")
	}
	return &pb.ReadMoviesResponse{
		Movies: movies,
	}, nil

}

func (*server) UpdateMovie(ctx context.Context, req *pb.UpdateMovieRequest) (*pb.UpdateMovieResponse, error){
	fmt.Println("update movie")
	var movie Movie
	reqMovie:=req.GetMovie()
	res:=DB.Model(&movie).Where("id=?",reqMovie.Id).Updates(Movie{Title: reqMovie.Title, Genre: reqMovie.Genre})
	if res.RowsAffected==0{
		return nil, errors.New("Movie not found")
	}
	return &pb.UpdateMovieResponse{
		Movie: &pb.Movie{
			Id: movie.ID,
			Title: movie.Title,
			Genre: movie.Genre,
		},

	},nil
}